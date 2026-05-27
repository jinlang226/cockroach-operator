FROM cockroachdb/cockroach:v25.2.12 AS cockroach-source
FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
COPY cockroach-operator-bin /cockroach-operator
COPY --from=cockroach-source /cockroach/cockroach /usr/local/bin/cockroach
ENTRYPOINT ["/cockroach-operator"]
