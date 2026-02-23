# Cockroach Operator on kind: Trace Logging Guide

This guide explains how to run your modified Cockroach operator on a kind cluster and collect trace logs for conformance work.

It focuses on two things:
- exact commands to run
- what each command does and why it is needed

## 1. Recommended mode

Use this mode first:
- run operator inside kind
- enable trace logging in the operator deployment
- export trace JSON from the pod

Reason: this is the closest to real deployment behavior and avoids local binary/path issues.

## 2. Prerequisites

- `kind`, `kubectl`, `docker`, `bazel` are installed
- you are in this repo root:

```bash
cd ~/cockroach-operator
```

## 3. Step-by-step (recommended)

### Step 1: create a kind cluster

```bash
kind create cluster --name crdb-dev
kubectl config use-context kind-crdb-dev
kubectl get nodes
```

What this does:
- creates a clean Kubernetes cluster named `crdb-dev`
- switches `kubectl` to this cluster
- verifies cluster is reachable

### Step 2: build and push your modified operator image

```bash
export DOCKER_REGISTRY=localhost:5001
export DOCKER_IMAGE_REPOSITORY=cockroach-operator
export APP_VERSION=dev
```

What this does:
- defines the target image name as `localhost:5001/cockroach-operator:dev`
- `APP_VERSION=dev` becomes the image tag used by the deployment templating

If you do not have a local registry on `localhost:5001`, create one:

```bash
docker run -d --restart=always -p "127.0.0.1:5001:5000" --name kind-registry registry:2
docker network connect kind kind-registry
```

What this does:
- starts a local OCI registry
- lets kind nodes pull from that registry

Now build and push:

```bash
bazel run --stamp --action_env=APP_VERSION=${APP_VERSION} //:push_operator_image
```

What this does:
- builds operator image from your current source tree
- pushes image to `${DOCKER_REGISTRY}/${DOCKER_IMAGE_REPOSITORY}:${APP_VERSION}`

### Step 3: deploy CRDs/RBAC/manager/webhook resources

```bash
make k8s/apply DEV_REGISTRY=localhost:5001 APP_VERSION=dev
```

What this does:
- renders and applies the default kustomize stack
- includes namespace, CRDs, RBAC, webhook config, and manager deployment
- manager image is set to your pushed tag (`dev`)

Verify deployment:

```bash
kubectl -n cockroach-operator-system get deploy,pods
kubectl -n cockroach-operator-system rollout status deploy/cockroach-operator-manager
```

### Step 4: enable trace logging in deployment

```bash
kubectl -n cockroach-operator-system set env deploy/cockroach-operator-manager \
  TRACE_LOG_ENABLED=true \
  TRACE_LOG_PATH=/tmp/operator-trace.json

kubectl -n cockroach-operator-system rollout restart deploy/cockroach-operator-manager
kubectl -n cockroach-operator-system rollout status deploy/cockroach-operator-manager
```

What this does:
- turns trace logging on
- writes trace JSON to `/tmp/operator-trace.json` inside the operator container
- restarts deployment so new env vars take effect

### Step 5: trigger reconcile

```bash
kubectl apply -f examples/example.yaml
```

What this does:
- creates/updates a `CrdbCluster` custom resource
- triggers reconcile loop and trace events

### Step 6: watch runtime logs

```bash
kubectl -n cockroach-operator-system logs -f deploy/cockroach-operator-manager
```

What this does:
- streams controller logs
- useful to see reconcile errors and event flow while trace file is being written

### Step 7: export trace JSON to local machine

```bash
POD=$(kubectl -n cockroach-operator-system get pod -l app=cockroach-operator -o jsonpath='{.items[0].metadata.name}')
kubectl -n cockroach-operator-system exec "$POD" -- ls -l /tmp/operator-trace.json
kubectl -n cockroach-operator-system cp "$POD:/tmp/operator-trace.json" ./operator-trace.json
```

What this does:
- finds the operator pod
- confirms trace file exists
- copies trace file to your local repo directory as `./operator-trace.json`

## 4. Common failures and fixes

### A) `namespaces "cockroach-operator-system" not found`

Cause:
- operator manifests were not applied yet

Fix:
- run `make k8s/apply DEV_REGISTRY=localhost:5001 APP_VERSION=dev`

### B) `cockroach: command not found`

Cause:
- common in local-process run mode (operator binary running on host without `cockroach` CLI in PATH)

Fix:
- prefer kind deployment mode in this guide
- or install `cockroach` binary on host and ensure PATH is set

### C) trace file not found

Cause:
- trace env vars not set on deployment
- pod not restarted after env update

Fix:
- rerun Step 4, then check with:

```bash
kubectl -n cockroach-operator-system describe deploy cockroach-operator-manager | rg TRACE_LOG
```

### D) many repeated reconcile traces with same failure pattern

Cause:
- reconcile keeps failing and requeueing (for example version check failure)

Fix:
- first fix the underlying actor failure in runtime log
- then regenerate trace for conformance baseline

## 5. Optional: local-process mode (advanced)

Use this only when you specifically need host-local trace file writes without `kubectl cp`.

Key tradeoff:
- easier local file access
- easier to hit host dependency issues (`cockroach` binary, webhook certs, namespace setup)

If you use this mode, keep it explicit in your test notes.
