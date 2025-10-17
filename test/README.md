# Test Support

## `crds/`

controller-runtime tests can point to this directory to install CRDs.

This is used for example by `pkg/idfile/controller_test.go`.

To generate the jobset CRDS, `git clone
git@github.com:kubernetes-sigs/jobset.git`, run `make manifests` from that repo,
then

```
kustomize build config/components/crd/ > $(THIS_REPO:?)/test/crds/jobset.yaml
```

## `emulated-smoke/`

A self-contained emulated training job. Built using in-line shell scripts that
also provide a reference implementation of the checkpointing worker api.

Run `./emulated-smoke/deploy.sh` with a kubeconfig pointing to your current
cluster. The cluster should have the `deploy/multitier/controller` applied,
using the real replication worker from
`gke-internal/gke-storage/checkpoint-replicator` (TODO: instructions on how to
do that)..

Parameters are passed through the environment; you'll need at the least to pass
the GCS bucket name to put backups, and probably your machine type.

```
BUCKET_NAME=${your-bucket} \
NODE_TYPE=${your-machine-type-eg:-nvidia-tesla-a100} \
 ./deploy.sh
```

See the defaults section in the script for other customizable options.

The test requires at least 2 nodes.

The training job (as a stateful set) will stay up after the test so that it may
be examined.

To tear everything down, add a `--delete` flag. This should be done between any
runs.
