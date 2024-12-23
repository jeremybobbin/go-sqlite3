package sqlite3

import (
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

type Driver struct{
}

type Connector struct {
	name        string
	driver      *Driver
	ConnContext func() context.Context
	register chan *Conn
	suspend, resume      chan struct{}
	locker *sync.RWMutex
}

type Conn struct {
	connector *Connector
	driver    *Driver
	ctl       chan job
	ctx       context.Context
	cancel    context.CancelFunc
	errs      [3]error
	done      chan struct{}
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
		name: name,
		driver: d,
		register: make(chan *Conn),
		suspend: make(chan struct{}),
		resume: make(chan struct{}),
	}

	go c.control()
	return &c, nil
}

func(c *Connector) control() {
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
			c.resume<-struct{}{}
		case 2:
			// the connector is making a second new connection
			// the first connection's controller is reading from the suspend channel
			// when we suspend the first connection, it sets the RWMutex on the driver
			// and then closes the resume channel
			c.suspend<-struct{}{}
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

func (c *Connector) Connect(dial context.Context) (driver.Conn, error) {
	var err error
	var pipes [4]*os.File

	cmd := exec.Command("sqlite3", "-quote", "-header", string(c.name))

	err = makePipes(pipes[:])
	if err != nil {
		return nil, err
	}

	cmd.Stdin = pipes[0]
	var stdin io.WriteCloser = pipes[1]

	cmd.Stdout = pipes[3]
	cmd.Stderr = pipes[3]
	var outerr io.ReadCloser = pipes[2]

	if err = cmd.Start(); err != nil {
		for _, f := range pipes {
			f.Close()
		}
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	conn := Conn{
		connector: c,
		driver:    c.driver,
		ctl:       make(chan job),
		done:      make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
	}

	w := make(chan []byte)
	r := make(chan job)

	c.register <- &conn

	go conn.write(stdin, w)
	go conn.read(outerr, r)
	go func() {
		conn.errs[0] = cmd.Wait()
		conn.cancel()
		conn.done <- struct{}{}
	}()

	select {
	case <-c.resume:
		go conn.control(r, w)
	case <-dial.Done():
		err = dial.Err()
		defer func() {
			<-c.resume
			go conn.control(r, w)
			if err := conn.Close(); err != nil {
				panic(err)
			}
		}()
	}

	return &conn, err
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// control routine
func (c *Conn) control(r chan job, w chan []byte) {
	defer c.cancel()
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
				select{
				case <-job.ctx.Done():
					// last job is finished
				case <-c.ctx.Done():
					job.cancel()
				}
			}
			c.connector.locker = &sync.RWMutex{}
			close(c.connector.resume)
			continue
		}

		select {
		case w <- <-job.ch:
			// query was received from job.ch & passed to the writer
		case <-c.ctx.Done():
			return
		}

		select {
		case r <- job:
		case <-c.ctx.Done():
			close(job.ch)
			return
		}
	}
}

// writer routine
func (c *Conn) write(stdin io.WriteCloser, w chan []byte) {
	for buf := range w {
		buf = append(buf, []byte("\n.print \"'''\"\n")...)
		if _, err := stdin.Write(buf); err != nil {
			c.errs[1] = err
			c.cancel()
			break
		}
	}

	stdin.Close()
	c.done <- struct{}{}
}

// reader routine
func (c *Conn) read(r io.ReadCloser, ch <-chan job) {
	var i, j, m int // r - record index - magic cookie level denoting end-of-query

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
		if ok {
			// channel is fresh
		} else if job, ok = <-ch; !ok {
			break
		}

		// if the buffer is 3/4ths its original capacity
		if len(buf) < refill {
			tmp := make([]byte, size)
			copy(tmp, buf[:j])
			buf = tmp
		}

		if i+m < j {
			// still got bytes left to process
		} else if n, err := r.Read(buf[j:]); err != nil {
			if err != io.EOF {
				c.errs[2] = err
			}
			c.cancel()
			break
		} else {
			j += n
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
			job.cancel()
			ok = false
			i += m
			m = 0
		}

		buf = buf[i:]
		j -= i
		i = 0

	}

	r.Close()
	c.done <- struct{}{}
	return
}

func (c *Conn) ResetSession(dial context.Context) error {
	select {
	case <-c.ctx.Done():
		return driver.ErrBadConn
	default:
		return nil
	}
}

func (c *Conn) IsValid(dial context.Context) bool {
	select {
	case <-c.ctx.Done():
		return false
	default:
		return true
	}
}

func (c *Conn) Ping(ctx context.Context) (err error) {
	var buf []byte = []byte{}

	job := job{
		ctx: ctx,
		ch:  make(chan []byte),
	}

	select {
	case c.ctl <- job:
	case <-c.ctx.Done():
		return driver.ErrBadConn
	case <-ctx.Done():
		return fmt.Errorf("while waiting for DB control channel: %w", ctx.Err())
	}

	select {
	case job.ch <- buf:
	case <-ctx.Done():
		return fmt.Errorf("while sending empty query to channel received from DB control: %w", ctx.Err())
	}

	select {
	case <-job.ch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("while waiting for empty query result: %w", ctx.Err())
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

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (c *Conn) Close() (err error) {
	close(c.ctl)
	<-c.done
	<-c.done
	<-c.done
	close(c.done)
	for _, err = range c.errs {
		if err != nil {
			break
		}
	}
	c.connector.register<-c
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
	case <-r.conn.ctx.Done():
		r.cancel()
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
					r.cancel()
					return io.EOF
				}
				r.i = 0
				continue
			case <-r.conn.ctx.Done():
				r.cancel()
				return io.ErrUnexpectedEOF
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

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	var query string
	var err error
	var r Result

	if query, err = subst1(s, args); err != nil {
		return nil, err
	}

	if locker := s.conn.connector.locker; locker != nil {
		locker.Lock()
		defer locker.Unlock()
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.conn = s.conn
	r.ch = make(chan []byte)

	select {
	case s.conn.ctl <- r.job:
		if locker := s.conn.connector.locker; locker != nil {
			locker.Lock()
			defer locker.Unlock()
		}
	case <-r.ctx.Done():
		return nil, driver.ErrBadConn
	}

	r.ch <- []byte(query)

	select {
	case s, ok := <-r.ch:
		if ok {
			return &r, fmt.Errorf("%s", string(s))
		}
		return &r, nil
	case <-s.conn.ctx.Done():
		r.cancel()
		return &r, s.conn.ctx.Err()
	case <-r.ctx.Done():
		return &r, r.ctx.Err()
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
		if locker := s.conn.connector.locker; locker != nil {
			locker.Lock()
			defer locker.Unlock()
		}
	case <-r.ctx.Done():
		return nil, driver.ErrBadConn
	}

	r.ch <- []byte(query)

	select {
	case s, ok := <-r.ch:
		if ok {
			return &r, fmt.Errorf("%s", string(s))
		}
		return &r, nil
	case <-s.conn.ctx.Done():
		r.cancel()
		return &r, s.conn.ctx.Err()
	case <-r.ctx.Done():
		return &r, ctx.Err()
	}
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	var query string
	var err error
	var rows Rows

	if query, err = subst1(s, args); err != nil {
		return nil, err
	}

	/*
	if locker := s.conn.connector.locker; locker != nil {
		locker.RLock()
		defer locker.RUnlock()
	}
	*/

	rows.ctx, rows.cancel = context.WithCancel(context.Background())
	rows.conn = s.conn
	rows.ch = make(chan []byte)

	select {
	case s.conn.ctl <- rows.job:
		if locker := s.conn.connector.locker; locker != nil {
			locker.RLock()
			defer locker.RUnlock()
		}
	case <-rows.ctx.Done():
		return nil, driver.ErrBadConn
	}

	rows.ch <- []byte(query)

	ch := make(chan []byte)
	go buffer(rows.ctx, rows.ch, ch)
	rows.ch = ch

	switch err := rows.Next(nil); err {
	case nil, io.EOF:
		return &rows, nil
	case io.ErrUnexpectedEOF, context.Canceled, context.DeadlineExceeded:
		return &rows, err
	default:
		panic(err)
	}
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var query string
	var err error
	var rows Rows

	if query, err = subst2(s, args); err != nil {
		return nil, err
	}

	rows.ctx, rows.cancel = context.WithCancel(ctx)
	rows.conn = s.conn
	rows.ch = make(chan []byte)

	select {
	case s.conn.ctl <- rows.job:
		if locker := s.conn.connector.locker; locker != nil {
			locker.RLock()
			defer locker.RUnlock()
		}
	case <-rows.ctx.Done():
		return nil, driver.ErrBadConn
	}

	rows.ch <- []byte(query)

	switch err := rows.Next(nil); err {
	case nil, io.EOF:
		return &rows, nil
	case io.ErrUnexpectedEOF, context.Canceled, context.DeadlineExceeded:
		return &rows, err
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
