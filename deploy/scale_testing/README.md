# Deployment Configuration

This directory holds configuration for deploying this feature to a cluster.

It is okay to `kubectl apply` this entire directory to get a full feature. It
will also be expected to deploy a subset, eg the csi driver without the
controller, in order to test local behavior. This is done by e2e tests. Because
of this, try to keep files functionally separated, eg rbac. In the future we may
move to separate directories per component.

## Components

* `csi_*`: the CSI ephemeral driver, ie the daemonset, rbac and the CSIDriver
  cluster resource.

## Usage

Rather than using kustomize or similar heavyweight templating solutions we're
going simple. Running the yaml through sed should be sufficient.

Note that currently the images are unversioned and always pull. This is nice for
debugging but may need to be tweaked for scale testing.

Variables like the namespace, brd size or topology configmap name are hardcoded
and may also need to be tweaked.

### Building

`make deploy-images PROJECT=${YOUR-IMAGE-REPO-PROJECT}`

### Variables to Replace

* `REPOSITORY` The full image repository from `make deploy-images`, probably
  `gcr.io/${YOUR-IMAGE-REPO-PROJECT}`.

### Example

`sed 's|REPOSITORY|gcr.io/${YOUR-IMAGE-REPO-PROJECT}|g' deploy/*.yaml | kubectl
apply -f -`
