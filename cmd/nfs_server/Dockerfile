FROM gke.gcr.io/debian-base:trixie-v1.0.0-gke.4@sha256:37fe6a16aabcbaf0dd2ff4fda39ce97806bec037d64a2a9aa93991c5f335ab36 AS debian

COPY ./cmd/nfs_server/startup.sh /

# Note that netbase is needed to get rpcbind working correctly.
# kmod is needed for modprobe. Need to hostpath /lib/modules.
RUN clean-install \
  mount bash nfs-common netbase nfs-kernel-server kmod

ENTRYPOINT ["/startup.sh"]
