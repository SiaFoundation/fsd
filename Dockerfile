# Use the official Go image to create a build artifact
FROM golang:1.17 AS builder

# Move to working directory
WORKDIR /app

# Copy Go modules and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build
COPY . .
RUN go build -o ipfsd .

# Use a minimal image for the final image
FROM alpine:latest

# Copy the build artifact into the image
COPY --from=builder /app/ipfsd /ipfsd

# Run the application
ENTRYPOINT ["/ipfsd"]
