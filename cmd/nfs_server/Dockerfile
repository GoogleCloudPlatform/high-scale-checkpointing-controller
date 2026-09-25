# We cannot use gke.gcr.io/debian-base as sysvinit-utils (/lib/lsb)
# appears to be crippled in that image.
# Auto-VC updates this digest when a new debian12 image is published.
FROM marketplace.gcr.io/google/debian12:latest@sha256:cf4d477c02debaf3ddbb464bdfa77a52eea0f45560caff759e6c0805f8c5d73d AS debian

COPY ./cmd/nfs_server/startup.sh /

# Note that netbase is needed to get rpcbind working correctly.
# kmod is needed for modprobe. Need to hostpath /lib/modules.
# dist-upgrade picks up bookworm-security fixes for packages that are already
# present in the base image.
RUN apt-get update && \
    apt-get dist-upgrade -y --no-install-recommends && \
    apt-get install -y --no-install-recommends \
      mount bash nfs-common netbase nfs-kernel-server kmod && \
    apt-get clean -y && \
    rm -rf \
      /var/cache/debconf/* \
      /var/lib/apt/lists/* \
      /var/log/* \
      /tmp/*

ENTRYPOINT ["/startup.sh"]