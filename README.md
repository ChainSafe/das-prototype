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
cargo run -- -n 500 -t uniform --timeout 6 sample --validators_number 2 --samples-per-validator 75 --parallelism 30
```

### Use snapshots
Snapshots allow saving network configurations along with various RNG seeds to have more consistent measurements and for debugging. Use `--snapshot` flag with values `new`, `last`, and specific timecode eg. `2022-11-08-11:46:09`. Snapshots are saved in `--cache-dir` folder default value = `./data`.

```bash
cargo run -- -n 500 --topology uniform --snapshot last disseminate -n 256
```
