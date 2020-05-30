SERVER_PATH=./server
CLIENT_PATH=./client
PROG=chord

.PHONY: server client fmt test clean

all: fmt test

server:
	go build -o ${SERVER_PATH}/${PROG} ${SERVER_PATH}

client:
	go build -o ${CLIENT_PATH}/${PROG} ${CLIENT_PATH}

fmt:
	go fmt ./...

test:
	go test -v

clean:
	$(RM) ${SERVER_PATH}/${PROG} ${CLIENT_PATH}/${PROG}
