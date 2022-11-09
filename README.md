# DiscV5 Testing and Simulations

This repo contains code for us to get familiar with the DiscV5 libraries in hopes of using it for DA.

## Usage

`cargo run -- -n <num-servers> -p <start-listen-port> -t <topology> [simulation-case] [ARGS]`

This will spin up `num-servers` of discv5 servers starting at port `start-listen-port` all the way to `start-listen-port` + `num-servers`, and then start `simulation-case`.

### Disseminate

```bash
cargo run -- -n 500 --topology uniform disseminate -n 256 --routing-strategy 'bucket-wise' --forward-mode 'FA' --replicate-mode 'RS' --redundancy 1
```

### Sample

```bash
cargo run -- -n 500 -t uniform --timeout 6 --snapshot last sample --validators_number 2 --samples-per-validator 75 --parallelism 30
```
