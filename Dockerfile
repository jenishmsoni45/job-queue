# App image: static queued, mockshop and loadgen binaries on an empty base, so there is no
# shell. Healthchecks run `queued healthcheck` and `mockshop healthcheck` instead.
FROM golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80 AS build

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
