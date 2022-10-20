use clap::Parser;
use cli_batteries::version;
use discv5::kbucket::Node;
use discv5::{
    enr,
    enr::{CombinedKey, Enr, NodeId},
    Discv5, Discv5Config, Discv5ConfigBuilder, Discv5Event, Key,
};
use enr::k256::elliptic_curve::bigint::Encoding;
use enr::k256::U256;
use itertools::Itertools;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use strum::EnumString;
use tokio::time;
use tokio_stream::{wrappers::ReceiverStream, StreamExt, StreamMap};
use tracing::{info, info_span, log::warn, trace_span, Instrument};
use warp::Filter;

mod das_tree;
mod types;

#[derive(Debug, PartialEq, EnumString)]
enum Topology {
    #[strum(serialize = "linear", serialize = "1")]
    Linear,
}

// struct Dv5Node {
//     discv5: Discv5,
//     das_tree: Arc<types::DasTree>,
// }

#[derive(Parser)]
pub struct Options {
    #[clap(long, short, default_value = "0.0.0.0")]
    ip_listen: String,
    #[clap(long, short, default_value = "9000")]
    port_udp: usize,
    #[clap(long, short, default_value = "10")]
    node_count: usize,
    #[clap(long, short, default_value = "linear")]
    topology: Topology,
    #[clap(long, short, default_value = "1")]
    simulation_case: SimCase,
}

fn main() {
    cli_batteries::run(version!(), app);
}

async fn app(options: Options) -> eyre::Result<()> {
    let discv5_servers = {
        let address = options.ip_listen.parse::<Ipv4Addr>().unwrap();
        construct_and_start(&options, address, options.port_udp, options.node_count).await
    };

    let enrs = Arc::new(
        discv5_servers
            .iter()
            .map(|s| s.local_enr())
            .collect::<Vec<_>>(),
    );

    let mut str = StreamMap::new();
    for (i, s) in discv5_servers.iter().enumerate() {
        let rec = ReceiverStream::new(s.event_stream().await.unwrap());
        str.insert(
            format!("Stream {} ({})", i, s.local_enr().node_id().to_string()),
            rec,
        );
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
                    Discv5Event::TalkRequest(req) => {
                        let req_msg = String::from_utf8(req.body().to_vec()).unwrap();
                        info!("Stream {}: Talk request received: {}", chan, req_msg);
                        let response = format!("Response: {}", req_msg);
                        req.respond(response.into_bytes()).unwrap();
                    }
                }
            }
        }
    });
    let enrs_stats = enrs.clone();
    let stats_task = tokio::spawn(async move {
        let enrs = enrs_stats;

        loop {
            play_simulation(&options, &discv5_servers, &enrs).await;

            let peer_count = discv5_servers
                .iter()
                .map(|s| s.connected_peers())
                .collect::<Vec<_>>();
            println!("Peer Count: {:?}", peer_count);
            // println!("Metrics: {:?}", met);
            sleep(Duration::from_secs(2))
        }
    });

    let enr_records = warp::path("enrs").map(move || format!("{:?}", &enrs.clone()));
    warp::serve(enr_records).run(([127, 0, 0, 1], 3030)).await;
    discv5_events_task.await.unwrap();
    stats_task.await.unwrap();
    Ok(())
}

#[derive(Debug, PartialEq, EnumString)]
enum SimCase {
    #[strum(serialize = "0")]
    SequentiaDiscovery,
    #[strum(serialize = "1")]
    LinearRouting,
    #[strum(serialize = "2")]
    ClosestToValue,
}

pub async fn play_simulation(
    opts: &Options,
    discv5_servers: &Vec<Discv5>,
    enrs: &Arc<Vec<Enr<CombinedKey>>>,
) {
    assert!(discv5_servers.len() > 2);
    info!("starting simulation case {:?}", opts.simulation_case);

    match opts.simulation_case {
        SimCase::SequentiaDiscovery => {
            let num_servers = discv5_servers.len();
            for (i, discv5) in discv5_servers.iter().enumerate() {
                let nextnode = enrs[(i + 1) % num_servers].clone().udp4().unwrap();
                if i != num_servers - 1 {
                    let predicate = Box::new(move |enr: &Enr<CombinedKey>| {
                        enr.udp4().unwrap() == nextnode.clone()
                    });

                    let found_nodes = discv5
                        .find_node_predicate(enrs[(i + 1) % num_servers].node_id(), predicate, 1)
                        .await
                        .unwrap();
                    println!("Found nodes: {:?}", found_nodes);
                }
            }

            // send talkreq from first node to last node
            let resp = discv5_servers[0]
                .talk_req(
                    enrs[enrs.len() - 1].clone(),
                    b"123".to_vec(),
                    format!("hello{}", 0).into_bytes(),
                )
                .await
                .unwrap();
            info!("Got response: {}", String::from_utf8(resp).unwrap());
            time::sleep(time::Duration::from_secs(5)).await;
        }
        SimCase::LinearRouting => {
            let last_node = discv5_servers.last().unwrap();
            let last_node_id = last_node.local_enr().node_id();
            let span = info_span!("routing", target_key = last_node_id.to_string());
            let last_node_upd4 = last_node.local_enr().udp4().unwrap().clone();
            // let predicate = Box::new(move |enr: &Enr<CombinedKey>| enr.udp4().unwrap() == last_node_upd4);
            let found = discv5_servers[0]
                .find_node(last_node_id)
                .instrument(span)
                .await
                .unwrap();
            info!("found_nodes {}", found.len());
            // send talkreq from first node to last node
            // let resp = discv5_servers[0].talk_req(enrs[enrs.len()-1].clone(), b"123".to_vec(), format!("hello{}",0).into_bytes()).await.unwrap();
            // info!("Got response: {}", String::from_utf8(resp).unwrap());
        }
        SimCase::ClosestToValue => {
            let last_node_id = discv5_servers.last().unwrap().local_enr().node_id();

            let distant_key = {
                let hash = U256::from_be_slice(last_node_id.raw().as_ref());
                let raw = hash ^ U256::ONE;
                NodeId::new(&raw.to_be_bytes())
            };

            let span = info_span!("routing", target_key = distant_key.to_string());
            let found = discv5_servers[0]
                .find_node(distant_key)
                .instrument(span)
                .await
                .unwrap();
            info!("found_nodes {}", found.len());
        }
    }
}

async fn construct_and_start(
    opts: &Options,
    listen_ip: Ipv4Addr,
    port_start: usize,
    node_count: usize,
) -> Vec<Discv5> {
    let mut discv5_servers = Vec::with_capacity(node_count);
    for i in 0..node_count {
        let listen_addr = format!("{}:{}", listen_ip, port_start + i)
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
            builder.ip4(listen_ip);
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
        let mut config_builder = Discv5ConfigBuilder::default();
        config_builder.request_retries(10);
        config_builder.filter_max_nodes_per_ip(None);
        let config = config_builder.build();

        // construct the discv5 server
        let discv5 = Discv5::new(enr, enr_key, config).unwrap();
        discv5_servers.push(discv5);
    }

    discv5_servers = set_topology(&opts, discv5_servers);

    for s in discv5_servers.iter_mut() {
        let ip4 = s.local_enr().ip4().unwrap();
        let udp4 = s.local_enr().udp4().unwrap();
        s.start(format!("{}:{}", ip4, udp4).parse().unwrap())
            .await
            .unwrap();
    }
    discv5_servers
}

pub fn set_topology(opts: &Options, mut discv5_servers: Vec<Discv5>) -> Vec<Discv5> {
    let last_node_id = Key::from(discv5_servers.last().unwrap().local_enr().node_id());

    match opts.topology {
        Topology::Linear => {
            // sort peers based on xor-distance to the latest node
            discv5_servers = discv5_servers
                .into_iter()
                .sorted_by_key(|s| Key::from(s.local_enr().node_id()).distance(&last_node_id))
                .rev()
                .collect::<Vec<_>>();

            for (i, s) in discv5_servers.iter().enumerate() {
                if i != discv5_servers.len() - 1 {
                    s.add_enr(discv5_servers[i + 1].local_enr().clone())
                        .unwrap()
                }
            }
        }
    }

    discv5_servers.iter().enumerate().for_each(|(i, s)| {
        println!(
            "{}.id={}, i-1={:?}, n={}, known={:?}, d={:?}",
            i,
            s.local_enr().node_id().to_string(),
            discv5_servers
                .get((i as i32 - 1) as usize)
                .map(|e| e.local_enr().node_id().to_string()),
            s.table_entries().len(),
            s.table_entries_enr()
                .first()
                .map(|e| e.node_id().to_string()),
            Key::from(s.local_enr().node_id()).distance(&last_node_id)
        );
    });

    discv5_servers
}
