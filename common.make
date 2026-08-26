.PHONY: verify unit-test prow-unit-test build-and-push deploy-images

DOCKER_QUIET?=--quiet

REPO_HOST?=us-docker.pkg.dev
REPO?=main

REPO_PATH?=$(REPO_HOST)/$(PROJECT)/$(REPO)

_repo_check=$(if $(findstring //,$(REPO_PATH)),$(error Must specify PROJECT or REPO_PATH))

verify:
	hack/verify-all.sh

# Omit packages without test files to avoid Go 1.25 covdata error (https://github.com/golang/go/issues/75031)
unit-test:
	@PKGS=$$(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./pkg/...) || exit 1; \
	if [ -n "$$PKGS" ]; then \
		go test -v=false -mod=vendor -timeout 20m -cover $$PKGS; \
	fi

build-and-push:
	$(call _repo_check)
	@if [ -z $(IMAGE) ] ; then echo Missing IMAGE; false; fi
	@if [ -z $(DOCKERFILE) ] ; then echo Missing DOCKERFILE; false; fi
	docker build $(DOCKER_QUIET) -t $(REPO_PATH)/$(IMAGE):$(TAG) . --file $(DOCKERFILE) $(BUILD_ARGS)
	docker push $(REPO_PATH)/$(IMAGE):$(TAG)

deploy-images:
	$(MAKE) IMAGE=checkpoint_csi_driver DOCKERFILE=cmd/csi_driver/Dockerfile TAG=latest build-and-push
	$(MAKE) IMAGE=checkpoint_tmpfs_driver DOCKERFILE=cmd/tmpfs_driver/Dockerfile TAG=latest build-and-push

