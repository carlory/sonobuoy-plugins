FROM golang:1.25-buster AS build

COPY ./pkg /src/pkg
WORKDIR /src
COPY go.* /src
ENV CGO_ENABLED=0
RUN go mod download

RUN --mount=type=cache,target=/root/.cache/go-build \
    go test -c -o ai-conformance.test ./...

FROM golang:1.25-buster
COPY --from=build /src/ai-conformance.test ai-conformance.test
COPY pkg/testdata testdata

CMD ["bash", "-c", "go tool test2json ./ai-conformance.test -test.v"]
