#[macro_use]
extern crate rocket;
extern crate core;

use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Write};
use discv5::{enr, enr::{CombinedKey, NodeId, Enr}, Discv5, Discv5Config, Discv5Event, Key, Discv5ConfigBuilder, ConnectionState, ConnectionDirection, TalkRequest};
use warp::Filter;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::AddAssign;
use std::rc::Rc;
use tokio::{select, time};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{info, log::warn, info_span, trace_span, Instrument};
use std::sync::{Arc};
use std::thread::sleep;
use std::time::Duration;
use ::libp2p::{identity, Multiaddr, PeerId};
use ::libp2p::multiaddr::Protocol::Tcp;
use cli_batteries::{version};
use clap::Parser;
use discv5::kbucket::{BucketIndex, KBucketsTable, Node, NodeStatus};
use enr::k256::elliptic_curve::bigint::Encoding;
use enr::k256::elliptic_curve::weierstrass::add;
use enr::k256::U256;
use futures::{pin_mut, FutureExt};
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use rand::Rng;
use sha3::{Keccak256, Digest};
use strum::EnumString;
use tokio::sync::{mpsc, RwLock};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::UnboundedReceiverStream;
use crate::libp2p::Libp2pService;
use crate::http_rpc::RpcMsg;

mod http_rpc;
mod libp2p;

#[derive(Clone, Debug, PartialEq, EnumString)]
enum Topology {
    #[strum(serialize = "linear", serialize = "1")]
    Linear,
    #[strum(serialize = "uniform", serialize = "2")]
    Uniform,
}

#[derive(Clone, Debug, PartialEq, EnumString)]
enum SimCase {
    #[strum(serialize = "0")]
    SequentiaDiscovery,
    #[strum(serialize = "1")]
    LinearRouting,
    #[strum(serialize = "2")]
    ClosestToValue,
    #[strum(serialize = "3")]
    BucketCast
}

#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum TalkWire {
    #[strum(serialize = "discv5")]
    Discv5,
    #[strum(serialize = "rpc")]
    HttpRPC,
    #[strum(serialize = "libp2p")]
    Libp2p
}

#[derive(Clone, Parser)]
pub struct Options {
    #[clap(long, short, default_value = "127.0.0.1")]
    ip_listen: String,
    #[clap(long, short, default_value = "9000")]
    port_udp: usize,
    #[clap(long, short, default_value = "10")]
    node_count: usize,
    #[clap(long, short, default_value = "linear")]
    topology: Topology,
    #[clap(long, short, default_value = "1")]
    simulation_case: SimCase,
    #[clap(long, default_value = "discv5")]
    talk_wire: TalkWire
}

pub struct DASNode {
    discv5: Discv5,
    libp2p: Libp2pService,
    samples: HashMap<NodeId, ()>,
}

impl DASNode {
    pub fn new(discv5: Discv5, libp2p: Libp2pService) -> Self {
        Self {
            discv5,
            libp2p,
            samples: Default::default(),
        }
    }
}

fn main() {
    cli_batteries::run(version!(), app);
}

async fn app(options: Options) -> eyre::Result<()> {
    let discv5_servers = {
        let address = options.ip_listen.parse::<Ipv4Addr>().unwrap();
        construct_and_start(&options, address, options.port_udp, options.node_count).await
    };

    let enrs = Arc::new(discv5_servers
        .iter()
        .map(|s| s.local_enr())
        .collect::<Vec<_>>());

    let mut das_nodes = vec![];

    let enr_to_libp2p = Arc::new(RwLock::new(HashMap::default()));

    for (i, discv5) in discv5_servers.into_iter().enumerate() {
        let mut events_str = ReceiverStream::new(discv5.event_stream().await.unwrap());
        let opts = options.clone();

        let (mut libp2p_worker, libp2p_msgs, libp2p_service) = {
            let keypair = identity::Keypair::generate_ed25519();
            let peer_id = PeerId::from(keypair.public());
            let mut addr = Multiaddr::from(IpAddr::from([127, 0, 0, 1]));
            addr.push(Tcp(4000+i as u16));

            enr_to_libp2p.write().await.insert(discv5.local_enr().node_id(), (peer_id, addr.clone()));

            libp2p::Libp2pDaemon::new(keypair, addr, i)
        };
        let mut libp2p_msgs = UnboundedReceiverStream::new(libp2p_msgs);
        let srv = Arc::new(RwLock::new(DASNode::new(discv5, libp2p_service)));
        das_nodes.push(srv.clone());
        let (tx, rx) = mpsc::channel(1);
        let mut rpc_str = ReceiverStream::new(rx);

        let talk_wire = opts.talk_wire.clone();
        tokio::spawn(async move {
            match talk_wire {
                TalkWire::HttpRPC => {
                    http_rpc::serve(tx, ([127, 0, 0, 1], 3000+i as u16)).await;
                }
                TalkWire::Libp2p => {
                    libp2p_worker.run().await;
                }
                _ => {}
            }
        });

        let addr_book = enr_to_libp2p.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    Some(e) = events_str.next() => {
                        let chan = format!("{i} {}", srv.read().await.discv5.local_enr().node_id().to_string());
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
                                info!("Stream {}: Talk request received", chan);
                                let resp = talk_response(req.body().to_vec(), srv.clone(), &opts, addr_book.clone()).await;
                                req.respond(resp);
                            },
                        }
                    },
                    Some(m) = rpc_str.next() => match m {
                        RpcMsg::TalkReq(body, tx) => {
                            info!("RPC {i}: Talk request received");
                            tx.send(talk_response(body, srv.clone(), &opts, addr_book.clone()).await);
                        }
                    },
                    Some(crate::libp2p::TalkReqMsg{resp_tx, peer_id, payload}) = libp2p_msgs.next() => {
                        info!("Libp2p {i}: Talk request received");
                            resp_tx.send(Ok(talk_response(payload, srv.clone(), &opts, addr_book.clone()).await));
                    }
                }
            }
        });
    }
    let enrs_stats = enrs.clone();
    let stats_task = tokio::spawn(async move {
        let enrs = enrs_stats;

        play_simulation(&options, &das_nodes, enr_to_libp2p.clone()).await;

        // let peer_count = das_nodes
        //     .iter()
        //     .map(|s| s.read().await.dht.connected_peers())
        //     .collect::<Vec<_>>();
        // println!("Peer Count: {:?}", peer_count);
    });

    stats_task.await.unwrap();

    tokio::signal::ctrl_c().await.unwrap();

    Ok(())
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
            discv5_servers = discv5_servers.into_iter()
                .sorted_by_key(|s| Key::from(s.local_enr().node_id()).distance(&last_node_id))
                .rev().collect::<Vec<_>>();

            for (i, s) in discv5_servers.iter().enumerate() {
                if i != discv5_servers.len() - 1 {
                    s.add_enr(discv5_servers[i+1].local_enr().clone()).unwrap()
                }
            }
        },
        Topology::Uniform => {
            let mut rng = rand::thread_rng();
            for (i, s) in discv5_servers.iter().enumerate() {
                let mut n = 128;
                while n != 0 {
                    let i = rng.gen_range(0usize..discv5_servers.len()-1);

                    match s.add_enr(discv5_servers[i].local_enr().clone()) {
                        Ok(_) => n-=1,
                        Err(_) => continue
                    }
                }
            }
        },
    }

    // discv5_servers.iter().enumerate().for_each(|(i,s)| {
    //     println!("{}.id={}, i-1={:?}, n={}, known={:?}, d={:?}",
    //              i,
    //              s.local_enr().node_id().to_string(),
    //              discv5_servers.get((i as i32 -1) as usize).map(|e| e.local_enr().node_id().to_string()),
    //              s.table_entries().len(),
    //              s.table_entries_enr().first().map(|e|e.node_id().to_string()),
    //              Key::from(s.local_enr().node_id()).distance(&last_node_id));
    // });

    discv5_servers
}

pub async fn play_simulation(
    opts: &Options,
    nodes: &Vec<Arc<RwLock<DASNode>>>,
    addr_book: Arc<RwLock<HashMap<NodeId, (PeerId, Multiaddr)>>>
) {
    match opts.simulation_case {
        SimCase::LinearRouting => {
            let last_node = nodes.last().unwrap().read().await;
            let last_node_id = last_node.discv5.local_enr().node_id();
            let span = info_span!("routing", target_key=last_node_id.to_string());
            // let last_node_upd4 = last_node.local_enr().udp4().unwrap().clone();
            // let predicate = Box::new(move |enr: &Enr<CombinedKey>| enr.udp4().unwrap() == last_node_upd4);
            let found = nodes[0].read().await.discv5.find_node(last_node_id).instrument(span).await.unwrap();
            info!("found_nodes {}", found.len());
            // send talkreq from first node to last node
            // let resp = discv5_servers[0].talk_req(enrs[enrs.len()-1].clone(), b"123".to_vec(), format!("hello{}",0).into_bytes()).await.unwrap();
            // info!("Got response: {}", String::from_utf8(resp).unwrap());
        }
        SimCase::ClosestToValue => {
            let last_node = nodes.last().unwrap().read().await;
            let last_node_id = last_node.discv5.local_enr().node_id();

            let distant_key = {
                let hash = U256::from_be_slice(last_node_id.raw().as_ref());
                let raw = hash ^ U256::ONE;
                NodeId::new(&raw.to_be_bytes())
            };

            let span = info_span!("routing", target_key=distant_key.to_string());
            let found = nodes[0].read().await.discv5.find_node(last_node_id).instrument(span).await.unwrap();
            info!("found_nodes {}", found.len());
        }
        SimCase::BucketCast => {
            let node = nodes[0].read().await;
            let local_view: HashMap<_, _> = node.discv5.kbuckets().buckets_iter()
                .map(|kb| kb.iter().map(|e| e.key.clone()).collect::<Vec<_>>())
                .enumerate().collect();

            let local_key = Key::from(node.discv5.local_enr().node_id());

            let keys = (0..256usize).map(|i| {
                let mut h = Keccak256::new();
                h.update(&i.to_be_bytes());
                NodeId::new(&h.finalize().try_into().unwrap())
            });

            let alloc = keys
                .map(|k| (BucketIndex::new(&local_key.distance(&Key::from(k))).unwrap().get(), Key::from(k)))
                .into_group_map();

            // alloc.clone().into_iter().filter(|(i, ks)| ks.len() > 0).for_each(|(i, keys)| info!(k={i}, keys=keys.len(), nodes=local_view.get(&i).unwrap().len()));

            let mut futures = vec![];
            for (k, keys) in alloc.into_iter() {
                let local_nodes = local_view.get(&k).unwrap();

                if let Some(next) = local_nodes.first() {
                    let node = nodes[0].read().await;
                    let enr = node.discv5.find_enr(next.preimage()).unwrap();
                    let addr_book = addr_book.clone();

                    futures.push(Box::pin(async move {
                        let msg = {
                            let mut m = vec![];
                            let mut w = BufWriter::new(&mut *m);
                            keys.iter().for_each(|k| { let _ = w.write(&k.hash.to_vec()); });
                            w.buffer().to_vec()
                        };

                        match opts.talk_wire {
                            TalkWire::Discv5 => {
                                let mut i = 0;
                                loop {
                                    let msg = {
                                        let mut m = vec![];
                                        let mut w = BufWriter::new(&mut *m);
                                        keys.iter().skip(i*29).take(29).for_each(|k| { let _ = w.write(&k.hash.to_vec()); });
                                        w.buffer().to_vec()
                                    };

                                    if msg.len() == 0 {
                                        break
                                    }

                                    i+=1;
                                    let _ = node.discv5.talk_req(enr.clone(), b"bcast".to_vec(), msg).await.map_err(|e| eyre::eyre!("{e}"));
                                }                            }
                            TalkWire::Libp2p => {
                                let (peer_id, addr) = addr_book.read().await.get(&enr.node_id()).unwrap().clone();
                                let _ = node.libp2p.talk_req(&peer_id, &addr, msg).await.unwrap();
                            }
                            TalkWire::HttpRPC => {
                                let _ = http_rpc::talk_req(enr.clone(), msg).await;
                            }
                        }
                    }));
                }
            }
            futures::future::join_all(futures).instrument(info_span!("bucket-cast")).await;

            let mut keys_per_node = HashMap::new();
            let mut nodes_per_key = HashMap::<_, usize>::new();

            for n in nodes {
                let n = n.read().await;
                keys_per_node.insert(n.discv5.local_enr().node_id(), n.samples.len());
                n.samples.keys().for_each(|k| {
                    nodes_per_key.entry(k.clone())
                        .and_modify(|e| e.add_assign(1))
                        .or_insert(1);
                })
            }

            // println!("Keys per Node:");
            // keys_per_node.iter().filter(|(_, keys)| **keys > 0).for_each(|(n, keys)| println!("node={} keys={keys}", n.to_string()));
            println!("Nodes per Key:");
            nodes_per_key.iter().for_each(|(k, nodes)| println!("key={} nodes={nodes}", k.to_string()));
            println!("Keys total: {}", nodes_per_key.len());
        }
        _ => {}
    }
}

pub async fn talk_response(
    msg: Vec<u8>,
    node: Arc<RwLock<DASNode>>,
    opts: &Options,
    addr_book: Arc<RwLock<HashMap<NodeId, (PeerId, Multiaddr)>>>
) -> Vec<u8> {
    match opts.simulation_case {
        SimCase::BucketCast => {
            let mut r = BufReader::new(&*msg);
            let mut keys = vec![];

            loop {
                let mut b = [0;32];
                if r.read(&mut b).unwrap() < 32 {
                    break
                }

                keys.push(NodeId::new(&b))
            }

            let local_view: HashMap<_, _> = node.read().await.discv5.kbuckets().buckets_iter()
                .map(|kb| kb.iter().map(|e| e.key.clone()).collect::<Vec<_>>())
                .enumerate().collect();

            // println!("Local view:");
            // local_view.iter().filter(|(_, v)|v.len() > 0).for_each(|(k,v)| println!("k={}, ns={}", *k, v.len()));

            let local_key = Key::from(node.read().await.discv5.local_enr().node_id());

            let alloc = keys.into_iter()
                .map(|k| (BucketIndex::new(&local_key.distance(&Key::from(k))).unwrap().get(), Key::from(k)))
                .into_group_map();

            // println!("Allocation:");
            // alloc.clone().into_iter().filter(|(i, ks)| ks.len() > 0).for_each(|(i, keys)| info!(k={i}, keys=keys.len(), nodes=local_view.get(&i).unwrap().len()));

            let mut futures = FuturesUnordered::new();

            for (k, keys) in alloc.into_iter() {
                let nodes = local_view.get(&k).unwrap();

                if let Some(next) = nodes.first() {
                    let enr = node.read().await.discv5.find_enr(next.preimage()).unwrap();

                    let msg = {
                        let mut m = vec![];
                        let mut w = BufWriter::new(&mut *m);
                        keys.into_iter().for_each(|k| { let _ = w.write(&*k.hash.to_vec()); });
                        w.buffer().to_vec()
                    };

                    {
                        let n = node.clone();
                        let addr_book = addr_book.clone();
                        futures.push(async move {
                            match opts.talk_wire {
                                TalkWire::Discv5 => {
                                    n.read().await.discv5.talk_req(enr, b"bcast".to_vec(), msg).await.map_err(|e| eyre::eyre!("{}", e))
                                }
                                TalkWire::Libp2p => {
                                    let (peer_id, addr) = addr_book.read().await.get(&enr.node_id()).unwrap().clone();
                                    n.read().await.libp2p.talk_req(&peer_id, &addr, msg).await
                                }
                                TalkWire::HttpRPC => {
                                    http_rpc::talk_req(enr, msg).await
                                }
                            }
                        });
                    }
                } else {
                    let mut n = node.write().await;
                    keys.into_iter().for_each(|k| {
                        n.samples.insert(k.preimage().clone(), ());
                    })
                }
            }

            while let Some(resp) = futures.next().await { resp.unwrap(); };
            return vec![]
        }
        _ => {
            let req_msg = String::from_utf8(msg).unwrap() ;
            let response = format!("Response: {}", req_msg);
            response.into_bytes()
        }
    }
}
