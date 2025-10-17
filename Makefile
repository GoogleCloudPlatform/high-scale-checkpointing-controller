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

.PHONY: all setup-prow prow-unit-test
.PHONY: clean-multitier multitier-images
.PHONY: grpc

MULTITIER_TAG=v0.0.21
BUILD_ARGS=

MULTITIER_DRIVER_NAME=multitier_driver
MULTITIER_STUB_NAME=multitier_stub
MULTITIER_CONTROLLER_NAME=multitier_controller
RANKS_IMAGE_NAME=ranks_server
NFS_SERVER_NAME=nfs_server

SCALE_TEST_TARGET=TestScaleMultitier
SCALE_TEST_COUNT=1
SCALE_TEST_TIMEOUT=2h

REPLICATION_WORKER_DEBUG_BACKUP=false

include common.make

all:
	@echo Select a target, eg verify

setup-prow:
	@if [ ! -d kubernetes ]; then \
		git clone https://github.com/kubernetes/kubernetes.git; \
		(cd kubernetes && ./hack/install-etcd.sh); \
		(cd kubernetes && make quick-release); \
	fi

prow-unit-test: setup-prow unit-test

grpc:
	for p in replication ranks; do \
		mkdir -p lib/grpc/$${p} ; \
		protoc --go_out=lib/grpc/$${p} --go_opt=paths=source_relative \
					 --go-grpc_out=lib/grpc/$${p} \
					 --go-grpc_opt=require_unimplemented_servers=false,paths=source_relative \
					 api/$${p}.proto ; \
	done

deploy/multitier/controller/controller-image.yaml:
	@if [ -z $(REPO_PATH) ] ; then echo "Missing REPO_PATH (or project)"; false; fi
	@if [ -z $(TAG) ] ; then echo Missing TAG. The same tag will be used for all images; false; fi
	@if [ -z $(MULTITIER_CONTROLLER_NAME) ] ; then echo Missing MULTITIER_CONTROLLER_NAME; false; fi
	@if [ -z $(RANKS_IMAGE_NAME) ] ; then echo Missing RANKS_IMAGE_NAME; false; fi
	sed "s|CONTROLLER_IMAGE_NAME|$(REPO_PATH)/$(MULTITIER_CONTROLLER_NAME)|" \
      deploy/multitier/controller/controller-image.yaml.template \
	 | sed "s|RANKS_IMAGE_NAME|$(REPO_PATH)/$(RANKS_IMAGE_NAME)|" \
   | sed "s|CONTROLLER_TAG|$(TAG)|" \
   > deploy/multitier/controller/controller-image.yaml

deploy/multitier/controller/csi-container-images.yaml:
	@if [ -z $(REPO_PATH) ] ; then echo Missing REPO_PATH; false; fi
	@if [ -z $(TAG) ] ; then echo Missing TAG. The same tag will be used for all images; false; fi
	@if [ -z $(MULTITIER_DRIVER_NAME) ] ; then echo Missing MULTITIER_DRIVER_NAME; false; fi
	sed "s|DRIVER_IMAGE_NAME|$(REPO_PATH)/$(MULTITIER_DRIVER_NAME):$(TAG)|" deploy/multitier/controller/csi-container-images.yaml.template \
   | sed "s|STUB_IMAGE_NAME|$(REPO_PATH)/$(MULTITIER_STUB_NAME):$(TAG)|" \
   | sed "s|NFS_SERVER_NAME|$(REPO_PATH)/$(NFS_SERVER_NAME):$(TAG)|" \
   > deploy/multitier/controller/csi-container-images.yaml

multitier-images:
	rm -f deploy/multitier/controller/controller-image.yaml
	rm -f deploy/multitier/controller/csi-container-images.yaml
	$(MAKE) TAG=$(MULTITIER_TAG) deploy/multitier/controller/controller-image.yaml deploy/multitier/controller/csi-container-images.yaml &
	$(MAKE) IMAGE=$(MULTITIER_DRIVER_NAME) BUILD_ARGS="--build-arg VERSION=$(MULTITIER_TAG)" \
          DOCKERFILE=cmd/multitier_driver/Dockerfile TAG=$(MULTITIER_TAG) build-and-push &
	$(MAKE) IMAGE=$(MULTITIER_STUB_NAME) DOCKERFILE=cmd/multitier_stub/Dockerfile TAG=$(MULTITIER_TAG) build-and-push &
	$(MAKE) IMAGE=${MULTITIER_CONTROLLER_NAME} DOCKERFILE=cmd/multitier_controller/Dockerfile TAG=$(MULTITIER_TAG) build-and-push &
	$(MAKE) IMAGE=$(NFS_SERVER_NAME) DOCKERFILE=cmd/nfs_server/Dockerfile TAG=$(MULTITIER_TAG) build-and-push &
	$(MAKE) IMAGE=$(RANKS_IMAGE_NAME) DOCKERFILE=cmd/multitier_ranks/Dockerfile TAG=$(MULTITIER_TAG) build-and-push &

clean-multitier:
	rm -f deploy/multitier/controller/{controller-image,csi-container-images}.yaml

scale-test-worker-image:
	@if [ -z $(TAG) ]; then echo Set TAG for scale test worker; false; fi
	$(MAKE) IMAGE=scale-test-worker DOCKERFILE=cmd/scale_test_worker/Dockerfile build-and-push

scale-test:
	@if [ -z $(TAG) ]; then echo Set TAG for scale test worker; false; fi
	@if [ -z $(REPO_PATH) ]; then echo Set REPO_PATH for scale test worker; false; fi
	@if [ -z $(SCALE_TEST) ]; then echo Set SCALE_TEST slice topology; false; fi
	@if [ -z $(TEST_BUCKET) ]; then echo Set TEST_BUCKET; false; fi
	MULTITIER_TEST_GCS_BUCKET=$(TEST_BUCKET) SCALE_TEST_IMAGE=$(REPO_PATH)/scale-test-worker:$(TAG) SCALE_TEST=$(SCALE_TEST) go test ./deploy_test -v -timeout $(SCALE_TEST_TIMEOUT) -run $(SCALE_TEST_TARGET) -count $(SCALE_TEST_COUNT)
