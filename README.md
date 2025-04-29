[![ipfsd](https://sia.tech/assets/banners/sia-banner-fsd.png)](http://sia.tech)

# fsd
[![GoDoc](https://godoc.org/go.sia.tech/fsd?status.svg)](https://godoc.org/go.sia.tech/fsd)
<a href="https://ipfs.tech"><img src="https://img.shields.io/badge/IPFS-Compatible-blue.svg" alt="IPFS Implementation"></a>
[![License](https://img.shields.io/github/license/SiaFoundation/fsd)](https://github.com/SiaFoundation/fsd/blob/main/LICENSE)

fsd is an IPFS node created by the Sia Foundation. It has optimizations specifically tailored for accessing data via the Sia network and is designed for large scale storage and retrieval. It is designed to be used with a backing `renter` node that is responsible for storing and managing data instead of storing data on a centralized service or on the local filesystem.

*This is currently a technology preview*

### TODO
- Add IPNS support

## Configuration

`fsd` is primarily configured via a YAML file. The configuration file should be placed in the working directory and named `fsd.yml`. 

### Example

```yml
api: # The API is used to pin and unpin CIDs and access metrics about the node
  address: :8081
  password: sia is cool
renterd: # The backing store for fsd. This configures the renterd node that stores the block data.
  workerAddress: http://localhost:9980/api/worker
  workerPassword: sia is extra cool
  busAddress: http://localhost:9980/api/bus
  busPassword: sia is extra cool
  bucket: ipfs
blockstore:
  maxConcurrent: 100 # the maximum number of concurrent fetches the blockstore will allow
  cacheSize: 1000000 # the maximum number of blocks that will be cached in memory at any given time
ipfs: # The IPFS node configuration.
  privateKey: # The private key for the IPFS node. If not set, a new key will be generated on startup and must be manually saved to the configuration file.
  gateway: # configure the HTTP gateway
    listenAddress: :8080
    redirectPathStyle: true
  fetch:
    enabled: false # enable or disable fetching blocks from the IPFS network. If false, will only serve pinned blocks.
    allowlist: [] # contains the CIDs that are allowed to be fetched remotely by the gateway. If empty, all CIDs are allowed.
  listenAddresses:
    - /ip4/0.0.0.0/tcp/4001 # the listen address for bitswap. Since no announce addresses are configured, no bits will be swapped
  announceAddresses:
    - /ipv4/127.0.0.1/tcp/4001 # the announce address for bitswap. This is the address that will be shared with other nodes on the network.
  provider:
    batchSize: 50000 # configures the reprovide batch size 
    interval: 18h # the interval at which the node will re-provide blocks to the IPFS network
```

## Build

`fsd` supports SQLite for its persistence. A gcc toolchain is required to build `fsd`.

```bash
CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w'  ./cmd/fsd
```

## Docker

`fsd` releases are available as Docker images hosted on `ghcr.io`. The image can be pulled from `ghcr.io/siafoundation/fsd`.

To setup `fsd`: copy the example `fsd.yml` to the directory you want to store `fsd` metadata, modify it to your needs, then create the container.

```yml
services:
  renterd:
    image: ghcr.io/siafoundation/renterd:latest
    ports:
      - 127.0.0.1:9980:9980/tcp
      - 9981:9981/tcp
    volumes:
      - renterd-data:/data
    restart: unless-stopped
  fsd:
    image: ghcr.io/siafoundation/fsd:latest
    depends_on:
      - renterd
    ports:
      - 8080:8080/tcp
      - 127.0.0.1:8081:8081/tcp
    volumes:
      - fsd-data:/data
    restart: unless-stopped

volumes:
  renterd-data:
  fsd-data:
```
