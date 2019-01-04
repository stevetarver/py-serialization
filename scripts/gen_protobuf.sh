#!/bin/sh -e
#
# Generate protobuf class from proto files
#
# Download the protobuf generator protoc from:
#   https://github.com/protocolbuffers/protobuf/releases
# Move the binary to your path
#   Note: you can brew install protobuf, but it adds python2/3 and other junk I don't want them doing
#         better to use a brute force install in case we need to add it to our build
# Python module is included in requirements: python3-protobuf
# Run this script
# Output is in TODO
if [ "$(uname -s)" = "Darwin" ]; then
    # If called through a symlink, this will point to the symlink
    THIS_SCRIPT_DIR="$( cd "$( dirname "${0}" )" && pwd )"
else
    THIS_SCRIPT_DIR=$(dirname $(readlink -f "${0}"))
fi
(
    # Work in repo root
    cd ${THIS_SCRIPT_DIR}/..

    protoc --python_out=./ node.proto
)
