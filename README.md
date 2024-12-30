go-driver for sqlite3, using the pre-compiled sqlite3 CLI

I was getting pretty tired of the cgo compile times for other sqlite drivers.

Current implementation writes the sql command to sqlite3's stdin, followed by `.print '''`,
which tells us when the output ends.

sqlite's stdout & stderr are set to be the same,
so any errors are printed before "'''".

A better way may involve named pipes & `.output`. Running the following:

```
mkfifo commands data
printf '%s\n' '.output data' 'SELECT * FROM table;' > commands &
echo .read commands | strace sqlite3
```


Gives the following (abridged) strace output:

```
read(0, ".read commands\n")
open("commands") = 4
read(4, ".output data\nSELECT * FROM table;\n")
open( "data") = 5
write(2, "Parse error near line 2: near \"t"...)
read(4, "")
close(4)
close(5)
```


sqlite3 provieds no way to configure the error output, however, it writes
the error message before it closes the data FD.

In C, with select, we'd be able to select over sqlite's stderr & the data pipe,
and if either:
- stderr is ready to read
- stderr is ready to read & data pipe is ready to read & returns EOF 

Under those conditions, we know there's an error & the error is only for us.

This repo proves that if data is written to 1 pipe & another is closed,
select will always report it in the correct order:
https://github.com/jeremybobbin/select-test

This is not possible in idiomatic go because go is for kids.
