# We cannot use gke.gcr.io/debian-base as sysvinit-utils (/lib/lsb)
# appears to be crippled in that image.
FROM debian:12 AS debian

COPY ./cmd/nfs_server/startup.sh /

# Note that netbase is needed to get rpcbind working correctly.
# kmod is needed for modprobe. Need to hostpath /lib/modules.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      mount bash nfs-common netbase nfs-kernel-server kmod && \
    apt-get clean -y && \
    rm -rf \
      /var/cache/debconf/* \
      /var/lib/apt/lists/* \
      /var/log/* \
      /tmp/*

ENTRYPOINT ["/startup.sh"]