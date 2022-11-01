#[macro_use]
extern crate rocket;
extern crate core;

use crate::http_rpc::RpcMsg;
use crate::libp2p::Libp2pService;
use ::libp2p::multiaddr::Protocol::Tcp;
use ::libp2p::{identity, Multiaddr, PeerId};
use clap::{Args, Parser, Subcommand};
use cli_batteries::version;
use discv5::kbucket::{BucketIndex, KBucketsTable, Node, NodeStatus};
use discv5::{
    enr,
    enr::{CombinedKey, Enr, NodeId},
    ConnectionDirection, ConnectionState, Discv5, Discv5Config, Discv5ConfigBuilder, Discv5Event,
    Key, TalkRequest,
};
use enr::k256::elliptic_curve::bigint::Encoding;
use enr::k256::elliptic_curve::weierstrass::add;
use enr::k256::U256;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{pin_mut, FutureExt};
use itertools::Itertools;
use nanoid::nanoid;
use rand::Rng;
use sha3::{Digest, Keccak256};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::AddAssign;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use strum::EnumString;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, RwLock};
use tokio::task::spawn_blocking;
use tokio::{select, time};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{debug, info, info_span, log::warn, trace_span, Instrument};
use warp::Filter;

mod http_rpc;
mod libp2p;
mod utils;

#[derive(Clone, Debug, PartialEq, EnumString)]
enum Topology {
    #[strum(serialize = "linear", serialize = "1")]
    Linear,
    #[strum(serialize = "uniform", serialize = "2")]
    Uniform,
}

#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum TalkWire {
    #[strum(serialize = "discv5")]
    Discv5,
    #[strum(serialize = "rpc")]
    HttpRPC,
    #[strum(serialize = "libp2p")]
    Libp2p,
}

// Defines settings how samples are forwarded when redundancy is enabled
#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum ForwardPolicy {
    // forward only the first message of the type
    #[strum(serialize = "F1")]
    ForwardOne,
    // forward every time the message is received
    #[strum(serialize = "FA")]
    ForwardAll,
}

// Defines settings how samples are replicated when redundancy is enabled
#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum ReplicatePolicy {
    // replicate only at the dispersal initiator
    #[strum(serialize = "R1")]
    ReplicateOne,
    // replicate at every step so a receiving node would also forward messages to certain number (based on --redundancy) of nodes in corresponding k-bucket
    #[strum(serialize = "RS")]
    ReplicateSome,
    // replicate at every step so a receiving node would also forward messages to every node in corresponding k-bucket
    #[strum(serialize = "RA")]
    ReplicateAll,
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
    #[clap(long, default_value = "discv5")]
    talk_wire: TalkWire,

    #[command(subcommand)]
    simulation_case: SimulationCase,
}

#[derive(Clone, clap::Subcommand)]
enum SimulationCase {
    Disseminate(DisseminationArgs),
    Sample(SamplingArgs),
}

#[derive(Clone, Args)]
struct DisseminationArgs {
    #[clap(long, short, default_value = "256")]
    number_of_samples: usize,
    #[clap(long, default_value = "F1")]
    forward_mode: ForwardPolicy,
    #[clap(long, default_value = "R1")]
    replicate_mode: ReplicatePolicy,
    #[clap(long, short, default_value = "1")]
    redundancy: usize,
}

#[derive(Clone, Args)]
struct SamplingArgs {
    #[clap(flatten)]
    dissemination: DisseminationArgs,
}

#[derive(Clone)]
pub struct DASNode {
    discv5: Arc<Discv5>,
    libp2p: Libp2pService,
    samples: Arc<RwLock<HashMap<NodeId, usize>>>,
    handled_ids: Arc<RwLock<HashMap<Vec<u8>, usize>>>,
}

impl DASNode {
    pub fn new(discv5: Discv5, libp2p: Libp2pService) -> Self {
        Self {
            discv5: Arc::new(discv5),
            libp2p,
            samples: Default::default(),
            handled_ids: Default::default(),
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

    let node_ids = discv5_servers
        .iter()
        .map(|e| e.local_enr().node_id())
        .collect::<Vec<_>>();

    let enrs = Arc::new(
        discv5_servers
            .iter()
            .map(|s| s.local_enr())
            .collect::<Vec<_>>(),
    );

    let mut das_nodes = vec![];

    let enr_to_libp2p = Arc::new(RwLock::new(HashMap::default()));
    let libp2p_to_enr = Arc::new(RwLock::new(HashMap::<PeerId, NodeId>::default()));

    let (msg_count_tx, msg_count_rx) = mpsc::unbounded_channel::<bool>();
    let msg_count = Arc::new(RwLock::new(0u64));
    {
        clone_all!(msg_count);
        tokio::spawn(async move {
            let mut msg_count = msg_count.write().await;
            let mut rx = UnboundedReceiverStream::new(msg_count_rx);
            loop {
                if let Some(e) = rx.next().await {
                    if e {
                        *msg_count += 1;
                    } else {
                        return;
                    }
                }
            }
        });
    }

    for (i, discv5) in discv5_servers.into_iter().enumerate() {
        let mut events_str = ReceiverStream::new(discv5.event_stream().await.unwrap());
        let opts = options.clone();

        let (mut libp2p_worker, libp2p_msgs, libp2p_service) = {
            let keypair = identity::Keypair::generate_ed25519();
            let peer_id = PeerId::from(keypair.public());
            let mut addr = Multiaddr::from(IpAddr::from([127, 0, 0, 1]));
            addr.push(Tcp(4000 + i as u16));

            enr_to_libp2p
                .write()
                .await
                .insert(discv5.local_enr().node_id(), (peer_id, addr.clone()));
            libp2p_to_enr
                .write()
                .await
                .insert(peer_id, discv5.local_enr().node_id());

            libp2p::Libp2pDaemon::new(keypair, addr, i)
        };
        let mut libp2p_msgs = UnboundedReceiverStream::new(libp2p_msgs);
        let srv = DASNode::new(discv5, libp2p_service);
        das_nodes.push(srv.clone());
        let (tx, rx) = mpsc::channel(1);
        let mut rpc_str = ReceiverStream::new(rx);

        let talk_wire = opts.talk_wire.clone();
        tokio::spawn(async move {
            match talk_wire {
                TalkWire::HttpRPC => {
                    http_rpc::serve(tx, ([127, 0, 0, 1], 3000 + i as u16)).await;
                }
                TalkWire::Libp2p => {
                    libp2p_worker.run().await;
                }
                _ => {}
            }
        });

        clone_all!(enr_to_libp2p, libp2p_to_enr, msg_count_tx, node_ids);
        tokio::spawn(async move {
            loop {
                select! {
                    Some(e) = events_str.next() => {
                        let chan = format!("{i} {}", srv.discv5.local_enr().node_id().to_string());
                        match e {
                            Discv5Event::Discovered(enr) => {
                                debug!("Stream {}: Enr discovered {}", chan, enr)
                            }
                            Discv5Event::EnrAdded { enr, replaced: _ } => {
                                debug!("Stream {}: Enr added {}", chan, enr)
                            }
                            Discv5Event::NodeInserted {
                                node_id,
                                replaced: _,
                            } => debug!("Stream {}: Node inserted {}", chan, node_id),
                            Discv5Event::SessionEstablished(enr, _) => {
                                debug!("Stream {}: Session established {}", chan, enr)
                            }
                            Discv5Event::SocketUpdated(addr) => {
                                debug!("Stream {}: Socket updated {}", chan, addr)
                            }
                            Discv5Event::TalkRequest(req) => {
                                debug!("Stream {}: Talk request received", chan);
                                msg_count_tx.send(true);
                                clone_all!(srv, opts, enr_to_libp2p, node_ids);
                                tokio::spawn(async move {
                                    let resp = handle_talk_request(req.node_id().clone(), req.body().to_vec(), srv, opts, enr_to_libp2p, node_ids, i).await;
                                    req.respond(resp);
                                });
                            },
                        }
                    },
                    Some(m) = rpc_str.next() => match m {
                        RpcMsg::TalkReq(body, tx) => {
                            debug!("RPC {i}: Talk request received");
                            msg_count_tx.send(true);
                            let from = node_ids[0];
                            clone_all!(srv, opts, enr_to_libp2p, node_ids);
                            tx.send(handle_talk_request(from, body, srv, opts, enr_to_libp2p, node_ids, i).await);
                        }
                    },
                    Some(crate::libp2p::TalkReqMsg{resp_tx, peer_id, payload}) = libp2p_msgs.next() => {
                        debug!("Libp2p {i}: Talk request received");
                        msg_count_tx.send(true);
                        let from = libp2p_to_enr.read().await.get(&peer_id).unwrap().clone();
                        clone_all!(srv, opts, enr_to_libp2p, node_ids);
                        tokio::spawn(async move {
                            resp_tx.send(Ok(handle_talk_request(from, payload, srv, opts, enr_to_libp2p, node_ids, i).await));
                        });
                    },
                }
            }
        });
    }
    let enrs_stats = enrs.clone();
    let stats_task = tokio::spawn(async move {
        let enrs = enrs_stats;

        play_simulation(&options, &das_nodes, enr_to_libp2p.clone(), node_ids).await;
        msg_count_tx.send(false).unwrap();
        let msg_count_total = *msg_count.read().await;
        info!("total messages sent = {msg_count_total} (communication overhead)");

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
        Topology::Uniform => {
            let mut rng = rand::thread_rng();
            for (i, s) in discv5_servers.iter().enumerate() {
                let mut n = 128;
                while n != 0 {
                    let i = rng.gen_range(0usize..discv5_servers.len() - 1);

                    match s.add_enr(discv5_servers[i].local_enr().clone()) {
                        Ok(_) => n -= 1,
                        Err(_) => continue,
                    }
                }
            }
        }
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
    nodes: &Vec<DASNode>,
    addr_book: Arc<RwLock<HashMap<NodeId, (PeerId, Multiaddr)>>>,
    node_ids: Vec<NodeId>,
) {
    match &opts.simulation_case {
        SimulationCase::Disseminate(args) => {
            let keys = (0..args.number_of_samples).map(|i| {
                let mut h = Keccak256::new();
                h.update(&i.to_be_bytes());
                NodeId::new(&h.finalize().try_into().unwrap())
            });

            disseminate_samples(keys, opts, &args, nodes, addr_book.clone(), &node_ids).await;

            let mut keys_per_node = HashMap::new();
            let mut nodes_per_key = HashMap::<_, usize>::new();

            for n in nodes {
                let samples = n.samples.read().await;
                keys_per_node.insert(n.discv5.local_enr().node_id(), samples.len());
                samples.keys().for_each(|k| {
                    nodes_per_key
                        .entry(k.clone())
                        .and_modify(|e| e.add_assign(1))
                        .or_insert(1);
                })
            }

            debug!("Keys per Node:");
            keys_per_node
                .iter()
                .filter(|(_, keys)| **keys > 0)
                .for_each(|(n, keys)| {
                    debug!(
                        "node={} ({}) keys={keys}",
                        n.to_string(),
                        node_ids.iter().position(|e| *e == *n).unwrap()
                    )
                });
            debug!("Nodes per Key:");
            nodes_per_key
                .iter()
                .for_each(|(k, nodes)| debug!("key={} nodes={nodes}", k.to_string()));
            debug!("Keys total: {}", nodes_per_key.len());

            let mut keys_stored_total = 0usize;
            keys_per_node
                .iter()
                .for_each(|(n, keys)| keys_stored_total += *keys);
            info!("total keys stored = {keys_stored_total} (storage overhead)");
        }
        SimulationCase::Sample(args) => {
            let keys = (0..256usize).map(|i| {
                let mut h = Keccak256::new();
                h.update(&i.to_be_bytes());
                NodeId::new(&h.finalize().try_into().unwrap())
            });

            disseminate_samples(
                keys,
                opts,
                &args.dissemination,
                nodes,
                addr_book.clone(),
                &node_ids,
            )
            .await;
        }
        _ => {}
    }
}

async fn disseminate_samples(
    keys: impl Iterator<Item = NodeId>,
    opts: &Options,
    args: &DisseminationArgs,
    nodes: &Vec<DASNode>,
    addr_book: Arc<RwLock<HashMap<NodeId, (PeerId, Multiaddr)>>>,
    node_ids: &Vec<NodeId>,
) {
    let node = nodes[0].clone();
    let local_view: HashMap<_, _> = node
        .discv5
        .kbuckets()
        .buckets_iter()
        .map(|kb| kb.iter().map(|e| e.key.clone()).collect::<Vec<_>>())
        .enumerate()
        .collect();

    let local_key = Key::from(node.discv5.local_enr().node_id());

    let alloc = keys
        .map(|k| {
            (
                BucketIndex::new(&local_key.distance(&Key::from(k)))
                    .unwrap()
                    .get(),
                Key::from(k),
            )
        })
        .into_group_map();

    let mut futures = vec![];
    let mut leftover_keys = vec![];
    for (k, mut keys) in alloc.into_iter() {
        let local_nodes = local_view.get(&k).unwrap().iter();
        let forward_to: Vec<_> = match args.replicate_mode {
            ReplicatePolicy::ReplicateOne => local_nodes.take(1).collect(),
            ReplicatePolicy::ReplicateSome => local_nodes.take(1 + args.redundancy).collect(),
            ReplicatePolicy::ReplicateAll => local_nodes.collect(),
        };

        if !forward_to.is_empty() {
            let batch_id = nanoid!(8).into_bytes();
            let msg = {
                let mut m = vec![];
                let mut w = BufWriter::new(&mut *m);
                w.write(&*batch_id).unwrap();
                keys.iter().for_each(|k| {
                    let _ = w.write(&k.hash.to_vec());
                });
                w.buffer().to_vec()
            };

            for next in forward_to {
                let node = nodes[0].clone();
                let enr = node.discv5.find_enr(next.preimage()).unwrap();
                let addr_book = addr_book.clone();

                {
                    let next_i = node_ids
                        .iter()
                        .position(|e| *e == *next.preimage())
                        .unwrap();
                    debug!(
                        "node {0} ({}) sends {} keys for request (id={}) to {next_i} ({})",
                        node.discv5.local_enr().node_id(),
                        keys.len(),
                        hex::encode(&batch_id),
                        next.preimage()
                    );
                }

                clone_all!(msg, keys);
                futures.push(Box::pin(async move {
                    match opts.talk_wire {
                        TalkWire::Discv5 => {
                            let mut i = 0;
                            loop {
                                let id = &*nanoid!(8).into_bytes();
                                let msg = {
                                    let mut m = vec![];
                                    let mut w = BufWriter::new(&mut *m);
                                    w.write(id.clone()).unwrap();
                                    keys.iter().skip(i * 29).take(29).for_each(|k| {
                                        let _ = w.write(&k.hash.to_vec());
                                    });
                                    w.buffer().to_vec()
                                };

                                if msg.len() == 8 {
                                    break;
                                }

                                i += 1;
                                let _ = node
                                    .discv5
                                    .talk_req(enr.clone(), b"bcast".to_vec(), msg)
                                    .await
                                    .map_err(|e| eyre::eyre!("{e}"));
                            }
                        }
                        TalkWire::Libp2p => {
                            let (peer_id, addr) =
                                addr_book.read().await.get(&enr.node_id()).unwrap().clone();
                            let _ = node.libp2p.talk_req(&peer_id, &addr, msg).await.unwrap();
                        }
                        TalkWire::HttpRPC => {
                            let _ = http_rpc::talk_req(enr.clone(), msg).await;
                        }
                    }
                }));
            }
        } else {
            warn!("fail to forward {} keys to {} bucket", keys.len(), k);
            leftover_keys.append(&mut keys);
        }
    }
    futures::future::join_all(futures)
        .instrument(info_span!("bucket-cast"))
        .await;

    let mut samples = nodes[0].samples.write().await;
    leftover_keys.into_iter().for_each(|e| {
        samples.insert(e.preimage().clone(), 1);
    });
}

pub async fn handle_talk_request(
    from: NodeId,
    msg: Vec<u8>,
    node: DASNode,
    opts: Options,
    addr_book: Arc<RwLock<HashMap<NodeId, (PeerId, Multiaddr)>>>,
    node_ids: Vec<NodeId>,
    node_idx: usize,
) -> Vec<u8> {
    let from_i = node_ids.iter().position(|e| *e == from).unwrap();
    match opts.simulation_case {
        SimulationCase::Disseminate(ref args) => {
            let mut r = BufReader::new(&*msg);
            let mut keys = vec![];

            let mut id = [0; 8];
            r.read(&mut id).unwrap();
            let id = id.to_vec();

            {
                debug!("node {node_idx} ({}) attempts to get lock for request (id={}) from {from_i} ({})", node.discv5.local_enr().node_id(), hex::encode(&id), from);
                let mut handled_ids = node.handled_ids.write().await;
                if handled_ids.contains_key(&id) && args.forward_mode != ForwardPolicy::ForwardAll {
                    debug!(
                        "node {node_idx} ({}) skipped request (id={}) from {from_i} ({})",
                        node.discv5.local_enr().node_id(),
                        hex::encode(&id),
                        from
                    );
                    return vec![];
                } else {
                    debug!(
                        "node {node_idx} ({}) received request (id={}) from {from_i} ({})",
                        node.discv5.local_enr().node_id(),
                        hex::encode(&id),
                        from
                    );
                    match handled_ids.entry(id.clone()) {
                        Entry::Occupied(mut e) => e.get_mut().add_assign(1),
                        Entry::Vacant(mut e) => {
                            e.insert(1);
                        }
                    };
                    drop(handled_ids);
                }
            }

            loop {
                let mut b = [0; 32];
                if r.read(&mut b).unwrap() < 32 {
                    break;
                }

                keys.push(NodeId::new(&b))
            }

            let local_view: HashMap<_, _> = node
                .discv5
                .kbuckets()
                .buckets_iter()
                .map(|kb| kb.iter().map(|e| e.key.clone()).collect::<Vec<_>>())
                .enumerate()
                .collect();

            // println!("Local view:");
            // local_view.iter().filter(|(_, v)|v.len() > 0).for_each(|(k,v)| println!("k={}, ns={}", *k, v.len()));

            let local_key = Key::from(node.discv5.local_enr().node_id());

            let alloc = keys
                .into_iter()
                .map(|k| {
                    (
                        BucketIndex::new(&local_key.distance(&Key::from(k)))
                            .unwrap()
                            .get(),
                        Key::from(k),
                    )
                })
                .into_group_map();

            let mut futures = FuturesUnordered::new();

            for (k, keys) in alloc.into_iter() {
                let local_nodes = local_view.get(&k).unwrap();
                /// if **replicate-all* then a receiver node applies forwards samples to more then one node in every k-bucket it handles
                let contacts_in_bucket = local_nodes.iter().filter(|e| *e.preimage() != from);
                let forward_to: Vec<_> = match args.replicate_mode {
                    ReplicatePolicy::ReplicateOne => contacts_in_bucket.take(1).collect(),
                    ReplicatePolicy::ReplicateSome => {
                        contacts_in_bucket.take(1 + args.redundancy).collect()
                    }
                    ReplicatePolicy::ReplicateAll => contacts_in_bucket.collect(),
                };

                if !forward_to.is_empty() {
                    for next in forward_to {
                        let enr = node.discv5.find_enr(next.preimage()).unwrap();

                        let next_i = node_ids
                            .iter()
                            .position(|e| *e == *next.preimage())
                            .unwrap();
                        debug!("node {node_idx} ({}) sends {} keys for request (id={}) to {next_i} ({})", node.discv5.local_enr().node_id(), keys.len(), hex::encode(&id), next.preimage());

                        let msg = {
                            let mut m = vec![];
                            let mut w = BufWriter::new(&mut *m);
                            w.write(&mut id.clone()).unwrap();
                            keys.clone().into_iter().for_each(|k| {
                                let _ = w.write(&*k.hash.to_vec());
                            });
                            w.buffer().to_vec()
                        };

                        {
                            clone_all!(node, addr_book, opts, id, keys);
                            futures.push(async move {
                                match opts.talk_wire {
                                    TalkWire::Discv5 => {
                                        node.discv5.talk_req(enr, b"bcast".to_vec(), msg).await.map_err(|e| eyre::eyre!("{e}"))
                                    }
                                    TalkWire::Libp2p => {
                                        let (peer_id, addr) = addr_book.read().await.get(&enr.node_id()).unwrap().clone();
                                        node.libp2p.talk_req(&peer_id, &addr, msg).await
                                    }
                                    TalkWire::HttpRPC => {
                                        http_rpc::talk_req(enr, msg).await
                                    }
                                }.map_err(|e| eyre::eyre!("error making request (id={}) from {node_idx} to {next_i}: {}", hex::encode(&id), e))
                            });
                        }
                    }
                } else {
                    let mut samples = node.samples.write().await;
                    keys.into_iter()
                        .for_each(|k| match samples.entry(k.preimage().clone()) {
                            Entry::Occupied(mut e) => e.get_mut().add_assign(1),
                            Entry::Vacant(mut e) => {
                                e.insert(1);
                            }
                        });
                }
            }

            while let Some(resp) = futures.next().await {
                resp.unwrap();
            }
            return vec![];
        }
        _ => {
            let req_msg = String::from_utf8(msg).unwrap();
            let response = format!("Response: {}", req_msg);
            response.into_bytes()
        }
    }
}
