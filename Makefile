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
rpm: VERSION=$(shell ./zeek-pdns version)
rpm:
	fpm -f -s dir -t rpm -n zeek-pdns -v $(VERSION) \
	--iteration=1 \
	--architecture native \
	--description "Zeek Passive DNS" \
	./zeek-pdns=/usr/bin/
