# v0.0.20 - Changelog since v0.0.19

- Reset client rank on jobset change.
- GCS Fuse best practice mount options

# v0.0.19 - Changelog since v0.0.18

- Remove thread contention and additional fixes to ranks server
- Refactor code generation for CRD and gRPC
- Update k8s deps to v0.33.0-beta.0
- Automatically create nodes in multitier deploy tests

# v0.0.18 - Changelog since v0.0.17

- use nconnect=16 for NFS connections
- change inMemoryVolumeSize in CheckpointConfigurations to take a resource
  string instead of an integer MiB, eg `inMemoryVolumeSize: 200GiB`. This is
  breaking, in that CheckpointConfigurations specifying `inMemoryVolumeSize:
  200000` will make a 200k and not 200G ramdisk.


# v0.0.17 - Changelog since v0.0.16

## Changes

- add cancellable retry to mountGCSBucket [gkecl/1275239](gkecl/1275239)
- separate rank assignment server. [gkecl/1272595](gkecl/1272595)

---

# v0.0.16 - Changelog since v0.0.15

## Changes

- use `skipCSIBucketAccessCheck` to improve scalability. [gkecl/1266235](gkecl/1266235)
