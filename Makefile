GO ?= go
KUBECTL ?= kubectl
IMG ?= influx-log-viewer:latest

.PHONY: fmt test vet build docker-build manifests validate deploy undeploy
fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/log-viewer ./cmd/log-viewer

docker-build:
	docker build -t $(IMG) .

manifests:
	$(KUBECTL) kustomize config

validate:
	$(KUBECTL) apply --dry-run=client -k config

deploy:
	$(KUBECTL) apply -k config

undeploy:
	$(KUBECTL) delete -k config --ignore-not-found
