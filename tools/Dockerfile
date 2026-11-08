# Tools image (D-021): every Go command runs here, so nothing is installed on the Mac.
# The official golang image, pinned by version and multi-arch index digest.
FROM golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80

# Pinned tool versions. `go install` checks each module against the Go checksum database.
ARG GOLANGCI_LINT_VERSION=v2.15.0
ARG GOVULNCHECK_VERSION=v1.9.0

# Never download another Go toolchain: the image's Go is the one go.mod pins.
ENV GOTOOLCHAIN=local

RUN go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}" \
 && go install "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}" \
 && rm -rf /root/.cache/go-build /go/pkg/mod

WORKDIR /src
