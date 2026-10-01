# The release image: the icetray release binary for the image's
# architecture on Ubuntu 24.04, whose glibc the release binaries are built
# against. The release workflow builds it from the binaries it publishes,
# unpacked under dist/<architecture>/, so an image and an archive of the
# same version hold the same binary. The configuration path comes from
# ICETRAY_CONFIG or --config, and the server listens where the file says;
# a container needs listen addresses other than the loopback defaults.

# icetray verifies TLS against the system's root certificates, and the
# stock image carries none. The bundle is the same on every architecture,
# so it is installed on the build platform and only the file is copied:
# the image runs no package manager and keeps no package state.
FROM --platform=$BUILDPLATFORM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 AS certificates
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

FROM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3

ARG TARGETARCH
COPY --from=certificates /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY dist/${TARGETARCH}/icetray /usr/local/bin/icetray

# The image's own unprivileged user, uid 1000.
USER ubuntu
EXPOSE 7433 9433
ENTRYPOINT ["/usr/local/bin/icetray"]
CMD ["serve"]
