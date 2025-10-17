#!/usr/bin/env bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# this example shows how to mount local ramdisk to a workload pod


set -o errexit
set -o nounset
set -o pipefail

HACK_DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")
ROOT=$(realpath "${HACK_DIR}/..")

cat <<EOF
Consider updating the local library by

go get -u google.golang.org/grpc@latest
go mod tidy && go mod vendor
EOF

echo toolchain check..
type -p protoc && type -p protoc-gen-go-grpc || cat <<EOF
Protobuf tool chain not found. Try

apt install -y protobuf-compiler
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
EOF
echo ..done

# protoc doesn't deal with absolute paths, so we cd and use relative paths.
cd "${ROOT}"

for file in ./proto/*.proto; do
  p=$(basename $file .proto)
  echo Generating ${p}
  mkdir -p ./lib/grpc/"${p}"
  protoc --go_out=./lib/grpc/"${p}" --go_opt=paths=source_relative \
         --go-grpc_out=./lib/grpc/"${p}" \
         --go-grpc_opt=require_unimplemented_servers=false,paths=source_relative \
         "${file}"
done
