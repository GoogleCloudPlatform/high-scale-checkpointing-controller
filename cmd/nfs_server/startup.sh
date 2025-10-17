#!/bin/bash
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


set -x

exports=${SHARED_DIRECTORY:?}

echo "Note rpcbind and nfs-common may start noisily, but failures seem to be ignorable."
service rpcbind start
service nfs-common start

echo "$exports *(rw,sync,fsid=0,insecure,no_root_squash,no_subtree_check)" >> /etc/exports
echo "Serving $exports"

modprobe nfs

service nfs-kernel-server start

stop () {
    service nfs-kernel-server stop
}

trap stop TERM

set +x
while true; do sleep 50m; done