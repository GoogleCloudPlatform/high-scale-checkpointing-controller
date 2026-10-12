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


set -e -x

exports=${SHARED_DIRECTORY:?}
mkdir -p "$exports"

echo "$exports *(rw,sync,fsid=0,insecure,no_root_squash,no_subtree_check)" >> /etc/exports
echo "Serving $exports"

modprobe nfs
modprobe nfsd

mkdir -p /run/rpc_pipefs /var/lib/nfs/v4recovery
mountpoint -q /proc/fs/nfsd || mount -t nfsd nfsd /proc/fs/nfsd
mountpoint -q /run/rpc_pipefs || mount -t rpc_pipefs rpc_pipefs /run/rpc_pipefs

/usr/sbin/rpcbind -w
/usr/sbin/rpc.statd --no-notify
/usr/sbin/rpc.idmapd

/usr/sbin/exportfs -r
/usr/sbin/rpc.mountd
/usr/sbin/nfsdctl autostart || /usr/sbin/rpc.nfsd

stop () {
    set +e
    /usr/sbin/nfsdctl threads 0 || /usr/sbin/rpc.nfsd 0
    /usr/sbin/exportfs -au
    /usr/sbin/exportfs -f
    kill "$(pidof rpc.mountd)" 2>/dev/null
    umount /proc/fs/nfsd 2>/dev/null
    exit 0
}

trap stop TERM INT

set +x
while true; do sleep 50m & wait $!; done
