# Chord DHT
A distributed hash table using the Chord lookup protocol, implemented in Go.

A demo can be found [here](https://youtu.be/Ym0kmfGoK8k)

## Goals
- Implement a functioning DHT
- Implement the Chord protocol for efficient node lookup
- Extend Chord with data replication to provide increased availability
- Learn gRPC and protocol buffers

## Prerequisites
Must have go version 1.12 or greater installed on the host in order to build.

## Build
To build the chord server and client, simply run: 
```
make server
make client
```
The executable named `chord` will be created in both the server and client folders.

## Configuration
Below are instructions for configuring your chord servers and clients.

### Server
In `./server/config.yaml` specify information about your server, like its ip and port. Here is an example:
```
addr: 172.0.0.1
port: 8001
logging: false
```
### Client
In `./client/config.yaml` specify the address of the chord server to send client requests to. Here is an example:
```
addr: 172.0.0.1:8001
```

## Run
Below are instructions for running a chord server or client.

### Server
To create a new chord ring:
```
./server/chord create
```
To join an existing chord ring:
```
./server/chord join <ip> <port>
```
### Client
To put a new key-value pair into the DHT:
```
./client/chord put <key> <val>
```
To get the value for a key in the DHT:
```
./client/chord get <key>
```
To locate the node responsible for a key in the DHT (for debugging purposes):
```
./client/chord locate <key>
```

## Local Development and Testing
To run multiple chord servers locally, run the test files in `./test`.
 
To start node1 listening on 0.0.0.0:8001, run
```
go run test/node1.go
```
To start node2 listening on 0.0.0.0:8002, run
```
go run test/node2.go
```
There are 5 test files, so this allows you to run 5 separate servers locally.
