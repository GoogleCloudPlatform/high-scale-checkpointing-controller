# High Scale Checkpointing

This repository contains the source code for components of High Scale
Checkpointing GKE backend.

## Deployments

* `deploy/multitier/README.md`: The multitier driver, as described in [the blog
  post](https://cloud.google.com/blog/products/ai-machine-learning/using-multi-tier-checkpointing-for-large-ai-training-jobs). Currently
  this stubs out the replication worker container and just works for testing.

## Directory Organization

* `pkg/`: go code implementing application logic
* `cmd/`: main.go and dockerfiles for binaries
* `proto/`: proto definitions (for gRPC)
* `apis/`: CRD IDL (commented go that defines CRD types and the base for
  generated code)
* `lib/`: generated code, performed by `./hack/update-codegen.sh`. No manual edits
  should be done in here.
* `crd/`: openapi CRD yaml, needs to be kept in sync with `apis/`
* `test/`: data used for testing. See the readme for details

## Setup Notes

As of October 2024, the transition to artifact registry is in full swing and
many projects have lost access to the legacy gcr.io registry. Full
instructions are at
[cloud.google.com/artifact-registry/docs/repositories](https://cloud.google.com/artifact-registry/docs/repositories),
but they seem to make things more complicated than necessary. In practice, the
steps seem to be:

* Go to [console.cloud.google.com](https://console.cloud.google.com), navigate
  to artifcat registry, and create a new repo. The default used here is one
  named "main" in `us-docker.pkg.dev`.
* Allow your local docker to push to this repo using gcloud authentication:
  `gcloud auth configure-docker us-docker.pkg.dev`
* You can then push and fetch images like
  `us-docker.pkg.dev/${YOUR_PROJECT}/main/some-image:tag`, the same as with the
  legacy repo.
* If you get `name unknown: Repository "main" not found` error, go to
  `us-docker.pkg.dev/${YOUR_PROJECT}` (which redirects to pantheon) in browser and
  create a new repository.

I may have missed some authentication steps so please update this if you have
problems.

## GCS Fuse Options

As of v0.0.21, gcs fuse mount options can be specified in the
CheckpointConfiguration. The options used can depend on the GKE cluster version,
see the [CSI driver version
requirements](https://cloud.google.com/kubernetes-engine/docs/concepts/cloud-storage-fuse-csi-driver#requirements)
for details.

The current recommended options are as follows. This requires at least 1.32.2-gke.1297001.

```
  implicit-dirs
  metadata-cache:negative-ttl-secs:0
  metadata-cache:ttl-secs:-1
  metadata-cache:stat-cache-max-size-mb:-1
  metadata-cache:type-cache-max-size-mb:-1
  file-cache:max-size-mb:-1
  file-cache:cache-file-for-range-read:true
  file-system:kernel-list-cache-ttl-secs:0
  file-cache:enable-parallel-downloads:true
  read_ahead_kb=1024
  write:enable-streaming-writes:true
```
