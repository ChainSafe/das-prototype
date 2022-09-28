use discv5::{
    enr,
    enr::{CombinedKey, NodeId, Enr},
    Discv5, Discv5Config, Discv5Event,
};
use std::net::{Ipv4Addr, SocketAddr};
use tokio::time;
use tokio_stream::{wrappers::ReceiverStream, StreamExt, StreamMap};
use tracing::{info, log::warn};
#[tokio::main]
async fn main() {
    // allows detailed logging with the RUST_LOG env variable
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))
        .unwrap();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter_layer)
        .try_init();

    // if there is an address specified use it
    let address = std::env::args()
        .nth(1)
        .map(|addr| addr.parse::<Ipv4Addr>().unwrap())
        .unwrap();

    let port = {
        if let Some(udp_port) = std::env::args().nth(2) {
            udp_port.parse().unwrap()
        } else {
            9000
        }
    };

    let node_count = {
        if let Some(node_count) = std::env::args().nth(3) {
            node_count.parse().unwrap()
        } else {
            10
        }
    };
    let discv5_servers = construct_and_start(address, port, node_count).await;
    let enrs = discv5_servers
        .iter()
        .map(|s| s.local_enr())
        .collect::<Vec<_>>();
    let mut str = StreamMap::new();
    for (i, s) in discv5_servers.iter().enumerate() {
        let rec = ReceiverStream::new(s.event_stream().await.unwrap());
        str.insert(format!("Stream {}", i), rec);
    }
    let discv5_events_task = tokio::spawn(async move {
        loop {
            while let Some((chan, e)) = str.next().await {
                match e {
                    Discv5Event::Discovered(enr) => {
                        info!("Stream {}: Enr discovered {}", chan, enr)
                    }
                    Discv5Event::EnrAdded { enr, replaced: _ } => {
                        info!("Stream {}: Enr added {}", chan, enr)
                    }
                    Discv5Event::NodeInserted {
                        node_id,
                        replaced: _,
                    } => info!("Stream {}: Node inserted {}", chan, node_id),
                    Discv5Event::SessionEstablished(enr, _) => {
                        info!("Stream {}: Session established {}", chan, enr)
                    }
                    Discv5Event::SocketUpdated(addr) => {
                        info!("Stream {}: Socket updated {}", chan, addr)
                    }
                    Discv5Event::TalkRequest(_) => info!("Stream {}: Talk request received", chan),
                }
            }
        }
    });

    let stats_task = tokio::spawn(async move {
        loop {
            let peer_count = discv5_servers
                .iter()
                .map(|s| s.connected_peers())
                .collect::<Vec<_>>();
            let met = discv5_servers
                .iter()
                .map(|s| s.metrics())
                .collect::<Vec<_>>();
            println!("Peer Count: {:?}", peer_count);
            println!("Metrics: {:?}", met);

            for (i, discv5) in discv5_servers.iter().enumerate() {
                if i != 2 {
                    let nextnode = enrs[(i+1)%3].clone().udp4().unwrap();
                    let predicate = Box::new(move |enr: &Enr<CombinedKey>| enr.udp4().unwrap() == nextnode.clone());
    
                    let found_nodes = discv5.find_node_predicate(enrs[(i+1)%3].node_id(), predicate, 1).await.unwrap();
                    println!("Found nodes: {:?}", found_nodes);
                }

            }
            time::sleep(time::Duration::from_secs(1)).await;
        }
    });

    discv5_events_task.await.unwrap();
    stats_task.await.unwrap();
}

async fn construct_and_start(
    listen_addr: Ipv4Addr,
    port_start: usize,
    node_count: usize,
) -> Vec<Discv5> {
    let mut discv5_servers = Vec::with_capacity(node_count);
    for i in 0..node_count {
        let listen_addr = format!("{}:{}", listen_addr, port_start + i)
            .parse::<SocketAddr>()
            .unwrap();
        println!("{}", listen_addr);
        let enr_key = CombinedKey::generate_secp256k1();
        let enr = {
            let mut builder = enr::EnrBuilder::new("v4");
            // TODO: Revisit this when we are not running locally
            // // if an IP was specified, use it
            // if let Some(external_address) = address {
            //     builder.ip4(external_address);
            // }
            // // if a port was specified, use it
            // if std::env::args().nth(2).is_some() {
            //     builder.udp4(port);
            // }
            builder.ip4(format!("127.0.0.1").parse().unwrap());
            builder.udp4(port_start as u16 + i as u16);
            builder.build(&enr_key).unwrap()
        };
        println!("Node Id: {}", enr.node_id());
        if enr.udp4_socket().is_some() {
            println!("Base64 ENR: {}", enr.to_base64());
            println!(
                "IP: {}, UDP_PORT:{}",
                enr.ip4().unwrap(),
                enr.udp4().unwrap()
            );
        } else {
            println!("ENR is not printed as no IP:PORT was specified");
        }
        // default configuration
        let config = Discv5Config::default();

        // construct the discv5 server
        let discv5 = Discv5::new(enr, enr_key, config).unwrap();
        discv5_servers.push(discv5);
    }
    let enrs = discv5_servers
        .iter()
        .map(|s| s.local_enr())
        .collect::<Vec<_>>();
    discv5_servers[0].add_enr(enrs[1].clone()).unwrap();
    discv5_servers[1].add_enr(enrs[2].clone()).unwrap();
    for s in discv5_servers.iter_mut() {
        let ip4 = s.local_enr().ip4().unwrap();
        let udp4 = s.local_enr().udp4().unwrap();
        s.start(format!("{}:{}", ip4, udp4).parse().unwrap())
            .await
            .unwrap();
    }
    discv5_servers
}
