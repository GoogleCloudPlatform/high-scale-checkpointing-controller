# Multitier Driver Manual Deployment

This implements the multitier GKE backend as described in [the replication
API](https://docs.google.com/document/d/1GMazl5jrctVPoQrP30z6jV_k9PPidVokUVqI9ZP4_PE/edit?tab=t.0)
and [go/gke-checkpoint-component](go/gke-checkpoint-component).

This is also known as phase 2 in [go/ml-model-checkpointing-execution-plan](go/ml-model-checkpointing-execution-plan).

This will be available as a managed GKE component. To deploy it manually, follow the steps below.

## Install non-managed Multitier CSI driver

1. Clean up any previously generated files

```bash
make clean-multitier
```

2. Generate new files and build images

```bash
make multitier-images REPO_PATH=<your-ar-path> MULTITIER_TAG=<your-custom-tag>
```

A `MULTITIER_TAG=` parameter can be added to set the tag (by default a version string is
used). If you need to change any defaults, run `make clean-multitier` and rerun
with different values.

A `REPLICATION_WORKER_DEBUG_BACKUP=` parameter can be added to enable debugging replication worker backup (by default set to false). If you need to change any defaults, run `make clean-multitier-controller` and rerun
with different values.  Note that `-k` must be used, and **not** `-f`.

3. Install the non-managed controller

```bash
kubectl apply -k deploy/multitier/controller
```

4. Install `CheckpointConfiguration` crd

```bash
kubectl apply -f crd/checkpointconfiguration.yaml
```

## Setup workload dependencies
To use the Multitier CSI driver, your workload will need to setup checkpointconfiguration and install dependencies.

1. Install JobSet crd

```bash
kubectl apply --server-side -f https://github.com/kubernetes-sigs/jobset/releases/download/v0.8.0/manifests.yaml
```

2. Grant the node csi driver service account access to your gcs bucket

```bash
gcloud storage buckets add-iam-policy-binding gs://$YOUR_GCS_BUCKET \
    --member "principal://iam.googleapis.com/projects/$YOUR_PROJECT_NUMBER/locations/global/workloadIdentityPools/$YOUR_PROJECT.svc.id.goog/subject/ns/gke-managed-checkpointing/sa/gke-checkpointing-multitier-node" \
    --role "roles/storage.objectUser"
```

3. You can apply a checkpoint configuration from our examples by using the following command

```bash
kubectl apply -f examples/checkpointconfiguration.yaml
```

4. Verify the daemonset is running successfully by running the command below.

```bash
kubectl get pod -n gke-managed-checkpointing
```

Example output
```
NAME                                                          READY   STATUS    RESTARTS   AGE
high-scale-checkpointing-controller-7c5d9bd446-svqqs          1/1     Running   0          2m59s
multitier-driver-d6cc2e82-385d-40ca-ab80-1a0879fd25cf-58tj7   5/5     Running   0          8s
multitier-driver-d6cc2e82-385d-40ca-ab80-1a0879fd25cf-jqrlk   5/5     Running   0          8s
multitier-driver-d6cc2e82-385d-40ca-ab80-1a0879fd25cf-rrv7k   5/5     Running   0          8s
```

## Deploy your workload
See `./example/emulated-jobset.yaml` for example workload using MTC CSI volume.

## Cleanup cluster when done using MTC CSI Driver
To clean up the resources, first delete the checkpointconfiguration

1. Delete any checkpoint configurations

```bash
kubectl delete checkpointconfigurations --all
```

2. Then delete other resources

```bash
kubectl delete namespace gke-managed-checkpointing
```

3. To remove customized yaml files

```bash
make clean-multitier
```