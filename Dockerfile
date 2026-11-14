# App image: static queued, mockshop and loadgen binaries on an empty base, so there is no
# shell. Healthchecks run `queued healthcheck` and `mockshop healthcheck` instead.
FROM golang:1.27.4-trixie@sha256:b54b2639de8c1bfe538ec5ffc45f29b1932235ac5f6d63c059853646677112d9 AS build

ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-buildvcs=false
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
RUN go build -trimpath -ldflags='-s -w' -o /out/queued ./cmd/queued \
 && go build -trimpath -ldflags='-s -w' -o /out/mockshop ./cmd/mockshop \
 && go build -trimpath -ldflags='-s -w' -o /out/loadgen ./cmd/loadgen

FROM scratch
COPY --from=build /out/queued /out/mockshop /out/loadgen /
# Non-root, numeric because the image has no /etc/passwd.
USER 65532:65532
ENTRYPOINT ["/queued"]
