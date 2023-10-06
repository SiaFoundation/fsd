[![ipfsd](https://sia.tech/assets/banners/sia-banner-ipfsd.png)](http://sia.tech)

# ipfsd

[![GoDoc](https://godoc.org/go.sia.tech/ipfsd?status.svg)](https://godoc.org/go.sia.tech/ipfsd)

IPFS gateway backed by `renterd` storage.

## usage

```sh
# calculate the CID of the Bored Ape 0 image
curl -X POST --data-binary @api0.png http://localhost:8001/api/cid/calculate
# Upload Bored Ape 0 image
curl -X POST --data-binary @ape0.png http://localhost:8001/api/upload/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ
"QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ"
curl http://localhost:8080/ipfs/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ
```
<image width="100px" src="https://ipfs.io/ipfs/QmRRPWG96cmgTn2qSzjwr2qvfNEuhunv6FNeMFGa9bx6mQ" />

## todo
- [ ] directory support, eg: ipfs://CID/images/cat.png, ipfs://QmeSjSinHpPnmXmspMjwiXyN6zS4E9zccariGR3jxcaWtq/0
