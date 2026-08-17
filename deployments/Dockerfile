# syntax=docker/dockerfile:1

# Multi-stage build for cmd/service, the courier-core flow engine's HTTP service.

FROM golang:1.24-alpine AS build

WORKDIR /src

# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /out/service \
    ./cmd/service

FROM gcr.io/distroless/static-debian12:nonroot AS runtime

WORKDIR /app

COPY --from=build /out/service ./service
COPY configs ./configs

ENV EIP_CONFIG=configs/example.yaml

USER nonroot:nonroot

EXPOSE 8080

ENTRYPOINT ["/app/service"]
