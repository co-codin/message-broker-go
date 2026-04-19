# syntax=docker/dockerfile:1

# ---- build stage ------------------------------------------------------------
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache dependencies first so source edits don't invalidate the module layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Static build so the runtime image can be distroless/scratch.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/minibroker .

# Pre-create an empty /data dir so we can COPY it into the distroless image
# with the right ownership (distroless has no shell to run mkdir/chown).
RUN mkdir -p /empty-data

# ---- runtime stage ----------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/minibroker /minibroker
COPY --from=build --chown=nonroot:nonroot /empty-data /data

# Broker client port
EXPOSE 4222
# Raft RPC port (used only when clustering is enabled)
EXPOSE 8001

# Persistent data dir. docker-compose mounts a named volume here.
VOLUME ["/data"]

USER nonroot:nonroot
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/minibroker", "-healthcheck"]
ENTRYPOINT ["/minibroker"]
CMD ["-dir", "/data"]
