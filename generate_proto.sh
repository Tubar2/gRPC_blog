#!/bin/bash

protoc blog/blogpb/blog.proto --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative