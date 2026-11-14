# Tools image (D-021): every Go command runs here, so nothing is installed on the Mac.
# The official golang image, pinned by version and multi-arch index digest.
FROM golang:1.27.4-trixie@sha256:b54b2639de8c1bfe538ec5ffc45f29b1932235ac5f6d63c059853646677112d9

# Pinned tool versions. `go install` checks each module against the Go checksum database.
ARG GOLANGCI_LINT_VERSION=v2.15.0
ARG GOVULNCHECK_VERSION=v1.9.0

# Never download another Go toolchain: the image's Go is the one go.mod pins.
ENV GOTOOLCHAIN=local

RUN go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}" \
 && go install "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}" \
 && rm -rf /root/.cache/go-build /go/pkg/mod

WORKDIR /src
