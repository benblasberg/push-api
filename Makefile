default:

protoc:
	protoc -I ./server/protobuf \
		--go_out ./server/protobuf/gen --go_opt paths=source_relative \
		--go-grpc_out ./server/protobuf/gen --go-grpc_opt paths=source_relative \
		./server/protobuf/server.proto