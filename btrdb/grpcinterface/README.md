To create new protobuf files in this folder:
```commandline
python -m grpc_tools.protoc --proto_path=./ --python_out=./ --pyi_out=.  --grpc_python_out=./ btrdb.proto 
```

Make sure the proto file is newest. And that you run the above command from within this folder.
