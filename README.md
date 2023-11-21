[![ipfsd](https://sia.tech/assets/banners/sia-banner-fsd.png)](http://sia.tech)

# fsd
[![GoDoc](https://godoc.org/go.sia.tech/fsd?status.svg)](https://godoc.org/go.sia.tech/fsd)
<a href="https://ipfs.tech"><img src="https://img.shields.io/badge/IPFS-Compatible-blue.svg" alt="IPFS Implementation"></a>

> **fsd** _(interplanetary **f**ile **s**ystem **d**aemon)_ is an IPFS daemon that stores blocks on Sia via `renterd`.

## usage

```sh
# calculate the CID of the Bored Ape 0 image
curl -X POST -H 'Content-Type: application/octet-stream' --data-binary @ape0.png http://localhost:8001/api/cid/calculate
# Upload Bored Ape 0 image
curl -X POST -H 'Content-Type: application/octet-stream' --data-binary @ape0.png http://localhost:8001/api/upload/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ
"QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ"
curl http://localhost:8080/ipfs/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ
```
<image width="100px" src="https://ipfs.io/ipfs/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ" />

## todo
- [ ] IPLD / directory support, eg: ipfs://CID/images/cat.png, ipfs://QmeSjSinHpPnmXmspMjwiXyN6zS4E9zccariGR3jxcaWtq/0
- [ ] IPNS support
- [ ] Better PIN support
- [ ] CAR support
