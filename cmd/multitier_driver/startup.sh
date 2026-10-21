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


set -e -u -x
# Unmatched globs expand to nothing (e.g. an empty /peers in stop()).
shopt -s nullglob

REMOTE_DIR=${REMOTE_DIR:-/remote}

function stop() {
    set +e
    # Use lazy unmounts (-l): they detach immediately without contacting the
    # NFS server, so a dead peer can't hang the unmount or fail it with EBUSY
    # and leave the pod's emptyDir impossible to tear down. Don't stat the
    # mountpoints (no `mountpoint`) for the same reason.
    echo "Unmounting /peers/*"
    for m in /peers/*; do
        umount -l "$m"
    done
    echo "Unmounting ${REMOTE_DIR}"
    umount -l "${REMOTE_DIR}"
    echo "Exiting ..."
    exit 0
}

# Clean up only when the container is asked to stop (SIGTERM on pod deletion).
# Don't trap EXIT: when multitier_driver crashes, the pod keeps running and the
# replication-worker shares /peers and /remote through bidirectional mount
# propagation. Unmounting them on a crash would silently disconnect
# replication and GCS backup until the next job.
trap stop TERM INT QUIT

/multitier_driver "$@" &

# Wait for the driver specifically: a bare `wait` always returns 0, which made a
# crashed driver look like a clean exit (container reported as Completed).
wait "$!"
