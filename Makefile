all: build test
build:
	go get -t -v ./...
	go build
test:
	go test -v ./...
static:
	go get -t -v ./...
	go build --ldflags '-extldflags "-static"'

.PHONY: rpm
rpm: build
rpm: VERSION=$(shell ./bro-pdns version)
rpm:
	fpm -f -s dir -t rpm -n bro-pdns -v $(VERSION) \
	--iteration=1 \
	--architecture native \
	--description "Bro Passive DNS" \
	./bro-pdns=/usr/bin/
