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

source "${HACK_DIR}/kube_codegen.sh"

PKG=$(grep ^module "${ROOT}"/go.mod | awk '{print $2}')

kube::codegen::gen_helpers \
    --boilerplate "${HACK_DIR}/boilerplate.go.txt" \
    "${ROOT}/apis"

kube::codegen::gen_register \
    --boilerplate "${HACK_DIR}/boilerplate.go.txt" \
    "${ROOT}/apis"

kube::codegen::gen_client \
    --with-watch \
    --output-dir "${ROOT}/lib/client" \
    --output-pkg "${PKG}/lib/client" \
    --boilerplate "${HACK_DIR}/boilerplate.go.txt" \
    "${ROOT}/apis"

