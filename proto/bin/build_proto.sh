# compile for python
#  protoc --proto_path=proto $(find proto/ros -iname "*.proto")  --python_out=.
./protoc --proto_path=../../proto $(find ../../proto -iname "*.proto")  --python_out=../../foxglove_backend/proto/py/
