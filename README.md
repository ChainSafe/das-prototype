# DiscV5 Testing and Simulations

This repo contains code for us to get familiar with the DiscV5 libraries in hopes of using it for DA. 

Usage:
`cargo run -- <listen-ip-addr> <start-listen-port> <num-servers>`
This will spin up `num-servers` amount of discv5 servers starting at port `start-listen-port` all the way to `start-listen-port` + `num-servers`. 

Example:
`cargo run -- 0.0.0.0 9000 10`
