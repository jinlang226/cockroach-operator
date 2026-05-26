FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
COPY cockroach-operator-bin /cockroach-operator
ENTRYPOINT ["/cockroach-operator"]
