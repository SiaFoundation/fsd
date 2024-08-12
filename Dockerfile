# Use the official Go image
FROM golang:1.22 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy Go modules and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code into the container
COPY . .

# Build the Go application
RUN go generate ./...
RUN CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w -linkmode external -extldflags "-static"' ./cmd/fsd

FROM scratch

COPY --from=builder /app/bin/* /usr/bin/

# Expose the default gateway port
EXPOSE 8080/tcp
# Expose the default API port
EXPOSE 8081/tcp

VOLUME ["/data"]

# Run the application
CMD ["fsd", "-dir", "/data"]
