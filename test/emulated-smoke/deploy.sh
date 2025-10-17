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


set -o nounset -o errexit

while [[ $# -gt 0 ]]; do
    case ${1} in
        --delete)
            delete=true
            ;;
        *)
            echo "unknown flag ${1}"
            echo "expect only optional --delete"
            exit 1
            ;;
    esac
    shift
done

cd $(dirname $(realpath ${BASH_SOURCE[0]}))
echo working from $(pwd)

# Defaults
NODE_TYPE=e2-standard-4
NODE_TAINT=nvidia.com/gpu
RAMDISK_MIB=10
NUM_NODES=2

subst() {
    sed "s|$1|$2|"
}

template() {
    subst BUCKET_NAME "${BUCKET_NAME}" | \
    subst NODE_TYPE "${NODE_TYPE}"     | \
    subst NODE_TAINT "${NODE_TAINT}"   | \
    subst RAMDISK_MIB "${RAMDISK_MIB}" | \
    subst NUM_NODES "${NUM_NODES}"
}

apply() {
    template < "$1" | kubectl apply -f -
}

in-logs() {
    kubectl logs "$1" 2>/dev/null | grep -q "$2"
}

restart-training() {
    kubectl scale statefulset emulated-training --replicas 0
    while [ $(kubectl get pod 2> /dev/null | wc -l) -gt 0 ]; do
          sleep 1
    done
    kubectl scale statefulset emulated-training --replicas ${NUM_NODES}
}

if [ ${delete:-}x = truex ]; then
    # Templating is not necessary for delete.
    kubectl delete -f worker-set.yaml
    kubectl delete -f scripts.yaml
    echo -n waiting for training pods to terminate...
    while [ $(kubectl get pod -l app=emulated-training --no-headers 2>/dev/null | wc -l ) -gt 0 ]; do
      sleep 1
    done
    echo finished
    kubectl delete -f config.yaml
    exit
fi

apply config.yaml
apply scripts.yaml
apply worker-set.yaml

timeout=60

echo "job started, waiting for first checkpoints"

tgt=emulated-training-$((NUM_NODES-1))
start=$(date +%s)
while ! in-logs ${tgt} "Starting at 0" || ! in-logs ${tgt} "Creating checkpoint 4"; do
      if [ $(($(date +%s) - $start)) -gt ${timeout} ]; then
          echo "timeout, failure"
          exit 1
      fi
      sleep 1s
done

echo "checkpoints found"
echo "Clearing local cache of ${tgt} and confirming restart from peers"

kubectl exec ${tgt} -- bash -c "rm -rvf /local/*"

restart-training

while ! in-logs ${tgt} "Starting at 4" || ! in-logs ${tgt} "Creating checkpoint 8"; do
      if [ $(($(date +%s) - $start)) -gt ${timeout} ]; then
          echo "timeout, failure"
          exit 1
      fi
      sleep 1s
done

echo "restart successful"

echo "smoke test succeeded!"
