FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libicu72 libreadline8 zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY pg-install/ /usr/local/

ENV PATH="/usr/local/bin:${PATH}"
ENV PGDATA=/var/lib/postgresql/data

COPY entrypoint.sh /usr/local/bin/crabka-compute-entrypoint

ENTRYPOINT ["crabka-compute-entrypoint"]
CMD ["postgres"]
