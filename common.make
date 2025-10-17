.PHONY: verify unit-test prow-unit-test build-and-push deploy-images

REPO_HOST?=us-docker.pkg.dev
REPO?=main

REPO_PATH?=$(REPO_HOST)/$(PROJECT)/$(REPO)

_repo_check=$(if $(findstring //,$(REPO_PATH)),$(error Must specify PROJECT or REPO_PATH))

verify:
	hack/verify-all.sh

unit-test:
	go test -v=false -mod=vendor -timeout 20m "./pkg/..." -cover

build-and-push:
	$(call _repo_check)
	@if [ -z $(IMAGE) ] ; then echo Missing IMAGE; false; fi
	@if [ -z $(DOCKERFILE) ] ; then echo Missing DOCKERFILE; false; fi
	docker build -t $(REPO_PATH)/$(IMAGE):$(TAG) . --file $(DOCKERFILE) $(BUILD_ARGS)
	docker push $(REPO_PATH)/$(IMAGE):$(TAG)

deploy-images:
	$(MAKE) IMAGE=checkpoint_csi_driver DOCKERFILE=cmd/csi_driver/Dockerfile TAG=latest build-and-push
	$(MAKE) IMAGE=checkpoint_tmpfs_driver DOCKERFILE=cmd/tmpfs_driver/Dockerfile TAG=latest build-and-push

