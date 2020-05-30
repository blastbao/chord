#!/bin/bash

set -xe

BASE=$GOPATH/src
CHORD=$BASE/github.com/cdesiniotis/chord

for file in $(find $CHORD -name "*.proto"); do
    protoc -I$BASE --go_out=plugins=grpc:$BASE $file
done