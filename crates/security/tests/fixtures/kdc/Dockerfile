FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        krb5-kdc krb5-admin-server krb5-user \
    && rm -rf /var/lib/apt/lists/*

# Seed configs are copied into place by setup.sh at container start so that a
# single bind-mount of this directory carries everything.
COPY krb5.conf kdc.conf setup.sh /seed/
RUN cp /seed/setup.sh /usr/local/bin/setup.sh && chmod +x /usr/local/bin/setup.sh

EXPOSE 88/tcp 88/udp
ENTRYPOINT ["/usr/local/bin/setup.sh"]
