# Use the official Go image
FROM golang:1.21 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy Go modules and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code into the container
COPY . .

# Build the Go application
RUN go generate ./...
RUN go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w' ./cmd/fsd

FROM debian:stable-slim

COPY --from=builder /app/bin/* /usr/bin/

# Expose the default gateway port
EXPOSE 8080/tcp
# Expose the default API port
EXPOSE 8081/tcp

VOLUME ["/data"]

# Run the application
CMD ["fsd", "-dir", "/data"]
