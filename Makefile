build:
	go build

vet:
	go vet

test:
	rm -f foo.db; go test -v
