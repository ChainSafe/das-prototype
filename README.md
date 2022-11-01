# DiscV5 Testing and Simulations

This repo contains code for us to get familiar with the DiscV5 libraries in hopes of using it for DA.

Usage:

`cargo run -- -n <num-servers> -p <start-listen-port> -t <topology> [simulation-case] [ARGS]`

This will spin up `num-servers` of discv5 servers starting at port `start-listen-port` all the way to `start-listen-port` + `num-servers`, and then start `simulation-case`.

Example:

`cargo run -- -n 500 --topology uniform disseminate -n 256`
