package sqlite3

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Driver struct {
}

type Connector struct {
	name            string
	driver          *Driver
	register        chan *Conn
	suspend, resume chan struct{}
	locker          *sync.RWMutex
}

type Conn struct {
	connector *Connector
	driver    *Driver
	ctl       chan job
	pipeline  context.Context
	cancel    context.CancelFunc
	errs      [3]error

	context.Context
}

type Stmt struct {
	query      string
	conn       *Conn
	semicolons []int
	questions  []int
}

type job struct {
	ch     chan []byte
	ctx    context.Context
	cancel context.CancelFunc
}

type Result struct {
	conn *Conn
	job
}

type Tx struct {
	*Conn
}

type Rows struct {
	Result
	Parser

	// names of rows
	names []string
}

type Parser struct {
	// parsing state
	s, i, n int // state, index into buf, n - number of rows processed

	// these are kept around to avoid re-allocating
	str  strings.Builder
	buf  []byte
	blob []byte
}

type ParseError struct {
	msg string
	Parser
}

func init() {
	sql.Register("sqlite3", &Driver{})
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	c := Connector{
		name:     name,
		driver:   d,
		register: make(chan *Conn),
		suspend:  make(chan struct{}),
		resume:   make(chan struct{}),
	}

	go c.control()
	return &c, nil
}

func (c *Connector) control() {
	conns := make(map[*Conn]struct{})
	var max int

	for conn := range c.register {
		if _, ok := conns[conn]; ok {
			delete(conns, conn)
			continue
		} else {
			conns[conn] = struct{}{}
		}

		if n := len(conns); n <= max {
			continue
		} else {
			max = n
		}

		switch max {
		case 1:
			c.resume <- struct{}{}
		case 2:
			// the connector is making a second new connection
			// the first connection's controller is reading from the suspend channel
			// when we suspend the first connection, it sets the RWMutex on the driver
			// and then closes the resume channel c.suspend<-struct{}{}
			c.suspend <- struct{}{}
		default:
			continue
		}
	}
}

func makePipes(p []*os.File) (err error) {
	if len(p)%2 != 0 {
		return fmt.Errorf("pipe array must be divisible by 2")
	}

	var i int
	for i = 0; i+1 < len(p) && err == nil; i += 2 {
		p[i], p[i+1], err = os.Pipe()
	}

	if err == nil {
		return
	}

	for i -= 3; i > 0; i-- {
		p[i].Close()
	}

	return
}

type ReadCloser struct {
	bufio.Reader
	f *os.File
}

func (r *ReadCloser) Close() error {
	return r.f.Close()
}

func (c *Connector) Connect(dial context.Context) (driver.Conn, error) {
	var err error
	var pipes [4]*os.File

	cmd := exec.Command("sqlite3", "-quote", "-header", string(c.name))

	if err = makePipes(pipes[:]); err != nil {
		return nil, err
	}

	cmd.Stdin = pipes[0]
	cmd.Stdout = pipes[3]
	cmd.Stderr = pipes[3]
	var stdin io.WriteCloser = pipes[1]
	var outerr io.ReadCloser = pipes[2]

	if err = cmd.Start(); err != nil {
		for _, f := range pipes {
			f.Close()
		}
		return nil, err
	} else {
		pipes[0].Close()
		pipes[3].Close()
	}

	ctx, mark := context.WithCancel(context.Background())

	// this is the context strictly for the stdin -> sqlite -> stdout pipeline
	pipeline, cancel := context.WithCancel(context.Background())

	conn := Conn{
		connector: c,
		driver:    c.driver,
		ctl:       make(chan job),
		Context:   ctx,
		pipeline:  pipeline,
		cancel:    cancel,
	}

	w := make(chan []byte)
	r := make(chan job)

	c.register <- &conn

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		conn.errs[0] = cmd.Wait()
		cancel()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		conn.errs[1] = conn.write(stdin, w)
		cancel()
		stdin.Close()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		conn.errs[2] = conn.read(outerr, r)
		cancel()
		outerr.Close()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		<-c.resume
		conn.control(pipeline, r, w)
		wg.Done()
	}()

	go func() {
		wg.Wait()
		c.register <- &conn // unregister
		mark()
	}()

	if err == nil {
		err = dial.Err()
	}

	if err != nil {
		cancel()
	}

	return &conn, err
}

func (c *Connector) Driver() driver.Driver {
	return c.driver

}

// control routine
func (c *Conn) control(ctx context.Context, r chan job, w chan []byte) {
	defer close(r)
	defer close(w)

	var job job
	var ok bool

	for {
		select {
		case job, ok = <-c.ctl:
			if !ok {
				return
			}
		case <-c.connector.suspend:
			c.connector.suspend = nil
			if ok { // job is valid
				<-job.ctx.Done()
				// last job is finished
			}
			c.connector.locker = &sync.RWMutex{}
			close(c.connector.resume)
			continue
		case <-ctx.Done():
			return
		}

		select {
		case w <- <-job.ch:
			// query was received from job.ch & passed to the writer
		case <-ctx.Done():
			return
		}

		select {
		case r <- job:
		case <-ctx.Done():
			return
		}
	}
}

// writer routine
func (c *Conn) write(stdin io.Writer, w chan []byte) error {
	t := "\n.print \"'''\"\n"
	var buf []byte

	for cmd := range w {
		n := len(t) + len(cmd)
		if len(buf) < n {
			buf = make([]byte, n)
			copy(buf[len(cmd):], t) // copy trailer to end of buffer
		}

		buf := buf[len(buf)-n:]
		copy(buf, cmd)

		if _, err := stdin.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

// reader routine
func (c *Conn) read(r io.Reader, ch <-chan job) error {
	var i, j, n, m int // r - record index - magic cookie level denoting end-of-query
	var err error

	const size int = 4096
	var refill int = int(math.Ceil(float64(size) / 4 * 3))
	var buf []byte = make([]byte, size)

	cookie := []byte("'''\n")
	var pc byte = '\n'
	var job job
	var ok bool

	if size < len(cookie)+1 { // +1 because cookie is preceded by a new line
		panic("buffer lenth must be greater than cookie size")
	}

	for {
		if !ok {
			job, ok = <-ch
		}

		// if the buffer is 3/4ths its original capacity
		if len(buf) < refill {
			tmp := make([]byte, size)
			copy(tmp, buf[:j])
			buf = tmp
		}

		if i+m < j && ok {
			// still got bytes left to process
		} else if n, err = r.Read(buf[j:]); err == io.EOF {
			if i+m < j {
				s := strings.TrimSpace(string(buf[i+m:j]))
				return fmt.Errorf("%s", s)
			}
			return nil
		} else if err != nil {
			return err
		} else {
			j += n
		}

		if !ok {
			continue
		}

		for i+m < j {
			c := buf[i+m]
			if m == 0 && pc != '\n' {
				i++
			} else if m >= len(cookie) {
				pc = c
				break
			} else if c == cookie[m] {
				m++
			} else {
				i += m + 1
				m = 0
			}
			pc = c
		}

		if i > 0 {
			select {
			case job.ch <- buf[:i]:
			case <-job.ctx.Done():
			}
		}

		if m >= len(cookie) {
			close(job.ch)
			ok = false
			i += m
			m = 0
		}

		buf = buf[i:]
		j -= i
		i = 0

	}
}

func (c *Conn) ResetSession(dial context.Context) error {
	select {
	case <-c.pipeline.Done():
		return driver.ErrBadConn
	default:
		return nil
	}
}

func (c *Conn) IsValid(dial context.Context) bool {
	select {
	case <-c.pipeline.Done():
		return false
	default:
		return true
	}
}

func (c *Conn) Ping(ctx context.Context) (err error) {
	var j job
	j.ctx, j.cancel = context.WithCancel(ctx)
	j.ch = make(chan []byte)

	select {
	case c.ctl <- j:
	case <-j.ctx.Done():
		return j.ctx.Err()
	case <-c.pipeline.Done():
		return driver.ErrBadConn
	}

	if locker := c.connector.locker; locker != nil {
		locker.Lock()
		defer locker.Unlock()
	}

	j.ch <- []byte{}

	select {
	case s, ok := <-j.ch:
		j.cancel()
		if s := string(s); ok && hasPrefixes(s, "Error", "Runtime error", "Parse error") {
			return fmt.Errorf("%s", string(s))
		}
		return nil
	case <-j.ctx.Done():
		return j.ctx.Err()
	case <-c.pipeline.Done():
		return driver.ErrBadConn
	}
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	var quotes, escaped bool
	visible := -1
	questions := make([]int, 0, 16)
	semicolons := make([]int, 0, 16)
	for i, c := range query {
		if quotes {
			switch c {
			case '\'':
				if escaped {
					// no-op
				} else {
					escaped = true
				}
			default:
				if escaped {
					quotes = false
				}
			}
		} else {
			switch c {
			case ' ', '\n', '\t', '\f', '\b', '\r':
			default:
				visible = i
			}
			switch c {
			case ';':
				semicolons = append(semicolons, i)
			case '?':
				questions = append(questions, i)
			case '\'':
				quotes = true
			default:
			}
		}
	}

	if n := len(semicolons); n <= 0 || visible > semicolons[n-1] {
		query += ";"
		semicolons = append(semicolons, len(query)-1)
	}

	return &Stmt{
		query:      query,
		conn:       c,
		semicolons: semicolons,
		questions:  questions,
	}, nil
}

func (c *Conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}

func (c *Conn) Close() (err error) {
	c.cancel()
	close(c.ctl)
	<-c.Done()
	for _, err = range c.errs {
		if err != nil {
			break
		}
	}
	return
}

func (c *Conn) Begin() (driver.Tx, error) {
	s, err := c.Prepare("BEGIN")
	if err != nil {
		return nil, err
	}

	_, err = s.Exec(nil)
	return &Tx{c}, err
}

func (c *Tx) Rollback() error {
	s, err := c.Prepare("ROLLBACK")
	if err != nil {
		return err
	}

	_, err = s.Exec(nil)
	return err
}

func (c *Tx) Commit() error {
	s, err := c.Prepare("COMMIT")
	if err != nil {
		return err
	}

	_, err = s.Exec(nil)
	return err
}

func (r *Result) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("unimplemented")
}

func (r *Result) RowsAffected() (int64, error) {
	return 0, fmt.Errorf("unimplemented")
}

func (r *Rows) Columns() []string {
	return r.names
}

func (r *Rows) Close() error {
	select {
	case <-r.ctx.Done():
	case <-r.conn.Done():
		for _, err := range r.conn.errs {
			if err != nil {
				return err
			}
		}
		return r.conn.Err()
	default:
		r.cancel()
	}
	return nil
}

func (e *ParseError) Error() string {
	var c rune = '?'
	if e.i < len(e.buf) {
		c = rune(e.buf[e.i])
	}
	return fmt.Sprintf("%s: index %d(char '%c') of %d in: \"%s\"", e.msg, e.i, c, len(e.buf), string(e.buf))
}

func (r *Rows) Next(dest []driver.Value) (err error) {
	var i, n, e, d int // i - dest index, n - int value, token index, e - exponent, d - decimal index
	var b byte
	var blob []byte
	var ok bool
	handle := func(s string) *ParseError {
		return &ParseError{
			msg:    s,
			Parser: r.Parser,
		}
	}

	const (
		NONE int = iota
		STRING
		X                       // start of blob literal
		BLOB                    // sqlite blob literal X'101010' -> \n\n\n ParseInt(s, 16, 8)
		SIGN                    // +/- preceding a number
		NUMERIC                 // we see digits, but no decimal - could be int or float
		DECIMAL                 // we saw the decimal, now expecting digits or e
		E                       // saw e, now expecting sign
		EXPONENT                // after value, expecting more digits, white space or ,
		NULL                    // NULL
		EOR                     // END OF RECORD
		PARSE                   // Parse error
		RUNTIME                 // Runtime error
		ESCAPED  = 0x10 << iota // white space after a value
		ERR                     // Error
	)

	for r.s != EOR {
		if r.i >= len(r.buf) {
			select {
			case r.buf, ok = <-r.ch:
				if !ok {
					return io.EOF
				}
				r.i = 0
				continue
			case <-r.conn.pipeline.Done():
				return io.ErrUnexpectedEOF
			case <-r.ctx.Done():
				return r.ctx.Err()
			}
		}

		c := r.buf[r.i]
		switch r.s {
		case NONE:
			switch c {
			case '-':
				n = -0
				r.s = NUMERIC
			case '+':
				n = 0
				r.s = NUMERIC
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				if n > 0 || n == 0 {
					n = int(c - '0')
				} else {
					n = 0 - int(c-'0')
				}
				r.s = NUMERIC
			case '.':
				r.s = DECIMAL
			case '\'':
				r.s = STRING
			case 'N':
				r.s = NULL
				n = 1
			case 'X':
				r.s = X
				n = 0
			case 'E':
				r.s = ERR
				n = 1
			case 'P':
				r.s = ERR | PARSE
				n = 1
			case 'R':
				r.s = ERR | RUNTIME
				n = 1
			case ',':
				return handle("expecting something before comma")
			default:
				return handle("expecting a number or a string")
			}
		case X:
			switch c {
			case '\'':
				blob = make([]byte, 0, 16)
				r.s = BLOB
			default:
				return handle("expecting a quote after X")
			}
		case BLOB:
			switch c {
			case '\'':
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				switch n {
				case 0:
					b = (c - '0') * 16
					n = 1
				case 1:
					b += c - '0'
					n = 0
					blob = append(blob, b)
				}
			case 'a', 'b', 'c', 'd', 'e', 'f':
				switch n {
				case 0:
					b = (c - 'a' + 10) * 16
					n = 1
				case 1:
					b += c - 'a' + 10
					n = 0
					blob = append(blob, b)
				}
			case 'A', 'B', 'C', 'D', 'E', 'F':
				switch n {
				case 0:
					b = (c - 'A' + 10) * 16
					n = 1
				case 1:
					b += c - 'A' + 10
					n = 0
					blob = append(blob, b)
				}
			case ',':
				dest[i] = blob
				i++
				r.s = NONE
				n = 0
			case '\n':
				dest[i] = blob
				i++
				r.s = EOR
				n = 0
			default:
				return handle(fmt.Sprintf("expecting a quote but got %c", c))
			}
		case NULL:
			null := "NULL"
			if n >= len(null) {
				// no-op
			} else if c != null[n] {
				return handle("NULL mispelled")
			} else {
				n++
				break
			}
			switch c {
			case ',':
				dest[i] = nil
				i++
				n = 0
				r.s = NONE
			case '\n':
				dest[i] = nil
				i++
				n = 0
				r.s = EOR
			}
		case ERR:
			var token string
			switch r.s & (^ERR) {
			case PARSE:
				token = "Parse error"
			case RUNTIME:
				token = "Runtime error"
			case 0:
				token = "Error"
			default:
				return handle("unexpected error condition")
			}
			if n >= len(token) {
				// no-op
			} else if c != token[n] {
				return handle("unexpected error token")
			} else {
				n++
			}
			switch c {
			case '\n':
				r.cancel()
				return fmt.Errorf("%s", r.str.String())
			default:
				r.str.WriteByte(c)
			}
		case STRING | ESCAPED:
			switch c {
			case '\'':
				r.str.WriteByte(c)
				r.s &= ^ESCAPED
			case ',':
				r.s = NONE
				s := r.str.String()
				r.str.Reset()
				if r.n == 0 {
					r.names = append(r.names, s)
					break
				}
				dest[i] = s
				i++
			case '\n':
				r.s = EOR
				s := r.str.String()
				r.str.Reset()
				if r.n == 0 {
					r.names = append(r.names, s)
					break
				} else {
					dest[i] = s
				}
				i++
			default:
				return handle(fmt.Sprintf("unexpected character: %c", c))
			}
		case STRING:
			switch c {
			case '\'':
				r.s |= ESCAPED
			default:
				r.str.WriteByte(c)
			}
		case NUMERIC:
			switch c {
			case '\n':
				dest[i] = n
				i++
				r.s = EOR
			case ',':
				dest[i] = n
				i++
				r.s = NONE
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				if n > 0 || n == 0 {
					n = (n * 10) + int(c-'0')
				} else {
					n = (n * 10) - int(c-'0')
				}
			case '.':
				r.s = DECIMAL
			default:
				return handle("expecting decimal, comma or white space")
			}
		case E:
			switch c {
			case '-':
				e = -0
				r.s = EXPONENT
			case '+':
				e = 0
				r.s = EXPONENT
			default:
				return handle("expecting sign")
			}
		case EXPONENT:
			switch c {
			case '\n':
				r.s = EOR
				dest[i] = float64(n) * math.Pow10(e-d)
				i++
				e = 0
			case ',':
				dest[i] = float64(n) * math.Pow10(e-d)
				i++
				e = 0
				r.s = NONE
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				if e < 0 || e == -0 {
					e = (e * 10) - int(c-'0')
				} else {
					e = (e * 10) + int(c-'0')
				}
			default:
				return handle("expecting numbers, white space or comma")
			}
		case DECIMAL:
			switch c {
			case '\n':
				dest[i] = float64(n) * math.Pow10(e-d)
				i++
				r.s = EOR
			case ',':
				dest[i] = float64(n) * math.Pow10(e-d)
				i++
				r.s = NONE
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				d++
				if n > 0 || n == 0 {
					n = (n * 10) + int(c-'0')
				} else {
					n = (n * 10) - int(c-'0')
				}
			case 'e':
				r.s = E
			}
		}
		r.i++
	}

	r.s = NONE
	r.n++
	return
}

func encode(w *strings.Builder, value any) error {
	switch v := value.(type) {
	case nil:
		w.WriteString("NULL")
	case string:
		w.WriteByte('\'')
		for _, c := range v {
			if c == '\'' {
				w.WriteRune('\'')
			}
			w.WriteRune(c)
		}
		w.WriteByte('\'')
	case int64:
		w.WriteString(strconv.FormatInt(v, 10))
	case bool:
		if v {
			w.WriteString("TRUE")
		} else {
			w.WriteString("FALSE")
		}
	case float64:
		w.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	case []byte:
		w.WriteString("base64('" + base64.StdEncoding.EncodeToString(v) + "')")
	case time.Time:
		w.WriteByte('\'')
		w.WriteString(v.Format("2006-01-02 15:04:05.999999999-07:00"))
		w.WriteByte('\'')
	default:
		return fmt.Errorf("unsupported type %T", v)
	}
	return nil
}

func subst1(s *Stmt, args []driver.Value) (string, error) {
	if l1, l2 := len(args), len(s.questions); l1 != l2 {
		return "", fmt.Errorf("got %d args but have %d question marks in the query: %s", l1, l2, s.query)
	} else if l1 == 0 {
		return s.query, nil
	}

	var buf strings.Builder
	buf.Grow(64)
	pq := 0 // index of previous question mark
	for i := 0; i < len(args); i++ {
		buf.WriteString(s.query[pq:s.questions[i]])
		pq = s.questions[i] + 1
		if err := encode(&buf, args[i]); err != nil {
			return buf.String(), err
		}
	}

	if pq > 0 && pq < len(s.query) {
		buf.WriteString(s.query[pq:])
	}

	return buf.String(), nil
}

func subst2(s *Stmt, args []driver.NamedValue) (string, error) {
	if l1, l2 := len(args), len(s.questions); l1 != l2 {
		return "", fmt.Errorf("got %d args but have %d question marks in the query: %s", l1, l2, s.query)
	} else if l1 == 0 {
		return s.query, nil
	}

	var buf strings.Builder
	buf.Grow(64)
	pq := 0 // index of previous question mark
	for i := 0; i < len(args); i++ {
		buf.WriteString(s.query[pq:s.questions[i]])
		pq = s.questions[i] + 1
		if err := encode(&buf, args[i].Value); err != nil {
			return buf.String(), err
		}
	}

	if pq > 0 && pq < len(s.query) {
		buf.WriteString(s.query[pq:])
	}

	return buf.String(), nil
}

// dynamic buffered channel
func buffer[T any](ctx context.Context, input, output chan T) {
	b := make([]T, 0, 8)

	var t T
	var out chan T = output

loop:
	for open := true; open || len(b) > 0; {
		if len(b) == 0 {
			out = nil
		} else {
			out = output
			t = b[0]
		}

		select {
		case t, open = <-input:
			if open {
				b = append(b, t)
			} else {
				input = nil
			}
		case out <- t:
			b = b[1:]
		case <-ctx.Done():
			break loop
		}

	}
	close(output)

	return
}

func hasPrefixes(needle string, haystack ...string) bool {
	for _, h := range haystack {
		if strings.HasPrefix(needle, h) {
			return true
		}
	}
	return false
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	var query string
	var err error
	var r Result

	if query, err = subst1(s, args); err != nil {
		return nil, err
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.conn = s.conn
	r.ch = make(chan []byte)

	select {
	case s.conn.ctl <- r.job:
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case <-s.conn.pipeline.Done():
		return nil, driver.ErrBadConn
	}

	if locker := s.conn.connector.locker; locker != nil {
		locker.Lock()
		defer locker.Unlock()
	}

	r.ch <- []byte(query)

	select {
	case s, ok := <-r.ch:
		r.cancel()
		if s := string(s); ok && hasPrefixes(s, "Error", "Runtime error", "Parse error") {
			return &r, fmt.Errorf("%s", string(s))
		}
		return &r, nil
	case <-r.ctx.Done():
		return &r, r.ctx.Err()
	case <-s.conn.pipeline.Done():
		return &r, driver.ErrBadConn
	}
}

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	var query string
	var err error
	var r Result

	if query, err = subst2(s, args); err != nil {
		return nil, err
	}

	r.ctx, r.cancel = context.WithCancel(ctx)
	r.conn = s.conn
	r.ch = make(chan []byte)

	select {
	case s.conn.ctl <- r.job:
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case <-s.conn.pipeline.Done():
		return nil, driver.ErrBadConn
	}

	if locker := s.conn.connector.locker; locker != nil {
		locker.Lock()
		defer locker.Unlock()
	}

	r.ch <- []byte(query)

	select {
	case s, ok := <-r.ch:
		r.cancel()
		if s := string(s); ok && hasPrefixes(s, "Error", "Runtime error", "Parse error") {
			return &r, fmt.Errorf("%s", string(s))
		}
		return &r, nil
	case <-r.ctx.Done():
		return &r, r.ctx.Err()
	case <-s.conn.pipeline.Done():
		return &r, driver.ErrBadConn
	}
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	var query string
	var err error
	var r Rows

	if query, err = subst1(s, args); err != nil {
		return nil, err
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.conn = s.conn
	r.ch = make(chan []byte)

	select {
	case s.conn.ctl <- r.job:
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case <-s.conn.pipeline.Done():
		return nil, driver.ErrBadConn
	}

	if locker := s.conn.connector.locker; locker != nil {
		locker.RLock()
		defer locker.RUnlock()
	}

	r.ch <- []byte(query)

	ch := make(chan []byte)
	go buffer(r.ctx, r.ch, ch)
	r.ch = ch

	switch err := r.Next(nil); err {
	case nil, io.EOF:
		return &r, nil
	case io.ErrUnexpectedEOF, context.Canceled, context.DeadlineExceeded:
		return &r, err
	default:
		panic(err)
	}
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var query string
	var err error
	var r Rows

	if query, err = subst2(s, args); err != nil {
		return nil, err
	}

	r.ctx, r.cancel = context.WithCancel(ctx)
	r.conn = s.conn
	r.ch = make(chan []byte)

	select {
	case s.conn.ctl <- r.job:
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case <-s.conn.pipeline.Done():
		return nil, driver.ErrBadConn
	}

	if locker := s.conn.connector.locker; locker != nil {
		locker.RLock()
		defer locker.RUnlock()
	}

	r.ch <- []byte(query)

	ch := make(chan []byte)
	go buffer(r.ctx, r.ch, ch)
	r.ch = ch

	switch err := r.Next(nil); err {
	case nil, io.EOF:
		return &r, nil
	case io.ErrUnexpectedEOF, context.Canceled, context.DeadlineExceeded:
		return &r, err
	default:
		panic(err)
	}
}

func (s *Stmt) NumInput() int {
	return len(s.questions)
}

func (s *Stmt) Close() error {
	return nil
}
