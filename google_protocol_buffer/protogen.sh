export GO111MODULE=on  # Enable module mode
go get google.golang.org/protobuf/cmd/protoc-gen-go \
         google.golang.org/grpc/cmd/protoc-gen-go-grpc
export PATH="$PATH:$(go env GOPATH)/bin"

printf "\nGENERATING PROTOBUF\n"
date
protoc --go_out=. *.proto --experimental_allow_proto3_optional
#!      `````````destinaiton dir```````name of proto``````proto3 flag``````````
#!protoc --proto_path=IMPORT_PATH --cpp_out=DST_DIR --java_out=DST_DIR --python_out=DST_DIR --go_out=DST_DIR --ruby_out=DST_DIR --objc_out=DST_DIR --csharp_out=DST_DIR path/to/file.proto
printf "\nGENRATING DONE\n"
