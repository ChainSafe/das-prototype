#[macro_use]
extern crate core;

use crate::libp2p::Libp2pService;
use crate::overlay::{DASContentKey, DASValidator};
use crate::utils::MsgCountCmd;
use ::libp2p::kad::store::MemoryStore;
use ::libp2p::multiaddr::Protocol::Tcp;
use ::libp2p::{identity, Multiaddr, PeerId};
use args::*;
use byteorder::{BigEndian, ReadBytesExt};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use cli_batteries::version;
use discv5::error::FindValueError;
use discv5::kbucket::{BucketIndex, KBucketsTable, Node, NodeStatus};
use discv5::{
    enr,
    enr::{CombinedKey, Enr, NodeId},
    kbucket, ConnectionDirection, ConnectionState, Discv5, Discv5Config, Discv5ConfigBuilder,
    Discv5Event, Key, RequestError, TalkRequest,
};
use discv5_overlay::{portalnet, utp};
use discv5_overlay::portalnet::discovery::{Discovery, NodeAddress};
use discv5_overlay::portalnet::overlay::{OverlayConfig, OverlayProtocol};
use discv5_overlay::portalnet::overlay_service::{
    OverlayCommand, OverlayRequest, OverlayRequestError, OverlayService,
};
use discv5_overlay::portalnet::storage::{
    ContentStore, DistanceFunction, MemoryContentStore, PortalStorage, PortalStorageConfig,
};
use discv5_overlay::portalnet::types::content_key::OverlayContentKey;
use discv5_overlay::portalnet::types::distance::{Distance, Metric, XorMetric};
use discv5_overlay::portalnet::types::messages::{Content, ElasticPacket, ProtocolId};
use discv5_overlay::utils::bytes::hex_encode_compact;
use discv5_overlay::utp::stream::{UtpListener, UtpListenerRequest};
use enr::k256::elliptic_curve::bigint::Encoding;
use enr::k256::elliptic_curve::weierstrass::add;
use enr::k256::U256;
use eyre::eyre;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{pin_mut, AsyncWriteExt, FutureExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use nanoid::nanoid;
use rand::prelude::StdRng;
use rand::{thread_rng, Rng, SeedableRng};
use sha3::{Digest, Keccak256};
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::AddAssign;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use std::{fs, iter};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::spawn_blocking;
use tokio::{select, time};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::log::error;
use tracing::{debug, info, info_span, log::warn, trace_span, Instrument};
use warp::Filter;
use ssz::{Decode, Encode};

mod args;
mod libp2p;
mod overlay;
mod utils;
mod messages;

const DAS_PROTOCOL_ID: &str = "DAS";
const DISSEMINATION_PROTOCOL_ID: &[u8] = b"D2S";

#[derive(Clone)]
pub struct DASNode {
    discovery: Arc<Discovery>,
    libp2p: Libp2pService,
    samples: Arc<RwLock<HashMap<NodeId, usize>>>,
    handled_ids: Arc<RwLock<HashMap<Vec<u8>, usize>>>,
    overlay: Arc<OverlayProtocol<DASContentKey, XorMetric, DASValidator, MemoryContentStore>>,
    active_requests: Arc<RwLock<HashMap<u16, oneshot::Sender<Vec<u8>>>>>
}

impl DASNode {
    pub fn new(
        discovery: Arc<Discovery>,
        utp_listener_tx: mpsc::UnboundedSender<UtpListenerRequest>,
        libp2p: Libp2pService,
    ) -> (
        Self,
        OverlayService<DASContentKey, XorMetric, DASValidator, MemoryContentStore>,
    ) {
        let config = OverlayConfig {
            bootnode_enrs: discovery.discv5.table_entries_enr(),
            // todo: setting low ping interval will hurt performance, investigate the impact of not having it
            ping_queue_interval: Some(Duration::from_secs(10000)),
            query_num_results: usize::MAX,
            query_timeout: Duration::from_secs(60),
            query_peer_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let protocol = ProtocolId::Custom(DAS_PROTOCOL_ID.to_string());
        let storage = {
            Arc::new(parking_lot::RwLock::new(MemoryContentStore::new(
                discovery.discv5.local_enr().node_id(),
                DistanceFunction::Xor,
            )))
        };
        let validator = Arc::new(DASValidator);

        let (overlay, service) = OverlayProtocol::new(
            config,
            discovery.clone(),
            utp_listener_tx,
            storage,
            Distance::MAX,
            protocol,
            validator,
        );

        (
            Self {
                discovery,
                libp2p,
                samples: Default::default(),
                handled_ids: Default::default(),
                overlay: Arc::new(overlay),
                active_requests: Arc::new(Default::default())
            },
            service,
        )
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

    let enr_to_libp2p = Arc::new(RwLock::new(
        HashMap::<NodeId, (PeerId, Multiaddr)>::default(),
    ));
    let libp2p_to_enr = Arc::new(RwLock::new(HashMap::<PeerId, NodeId>::default()));

    let (msg_counter, msg_count_rx) = mpsc::unbounded_channel::<MsgCountCmd>();
    {
        tokio::spawn(async move {
            let mut rx = UnboundedReceiverStream::new(msg_count_rx);
            let mut messages = 0u64;
            loop {
                if let Some(c) = rx.next().await {
                    match c {
                        MsgCountCmd::Increment => {
                            messages += 1;
                        }
                        MsgCountCmd::Reset => {
                            messages = 0;
                        }
                        MsgCountCmd::Get(tx) => tx.send(messages).unwrap(),
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
        let discovery = Arc::new(Discovery::new_raw(discv5, Default::default()));
        let (utp_events_tx, utp_listener_tx, mut utp_listener_rx, mut utp_listener) =
            UtpListener::new(discovery.clone());
        tokio::spawn(async move { utp_listener.start().await });
        let (das_node, overlay_service) = DASNode::new(discovery, utp_listener_tx, libp2p_service);
        das_nodes.push(das_node.clone());

        let talk_wire = opts.wire_protocol.clone();
        if talk_wire == TalkWire::Libp2p {
            tokio::spawn(async move {
                libp2p_worker.run().await;
            });
        }
        clone_all!(enr_to_libp2p, libp2p_to_enr, msg_counter, node_ids);
        tokio::spawn(async move {
            let mut overlay_service = overlay_service;
            let mut bucket_refresh_interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                select! {
                    Some(e) = events_str.next() => {
                        let chan = format!("{i} {}", das_node.discovery.discv5.local_enr().node_id().to_string());
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
                            Discv5Event::SessionEstablished(enr, socket_addr) => {
                                debug!("Stream {}: Session established {}", chan, enr);
                                das_node.discovery.node_addr_cache
                                    .write()
                                    .put(enr.node_id(), NodeAddress { enr, socket_addr });
                            }
                            Discv5Event::SocketUpdated(addr) => {
                                debug!("Stream {}: Socket updated {}", chan, addr)
                            }
                            Discv5Event::TalkRequest(req) => {
                                debug!("Stream {}: Talk request received", chan);
                                msg_counter.send(MsgCountCmd::Increment);
                                clone_all!(das_node, opts, enr_to_libp2p, node_ids, utp_events_tx);
                                tokio::spawn(async move {
                                    let protocol = ProtocolId::from_str(&hex::encode_upper(req.protocol())).unwrap();

                                    if protocol == ProtocolId::Utp {
                                        utp_events_tx.send(req).unwrap();
                                        return;
                                    }

                                    if protocol == ProtocolId::Custom(DAS_PROTOCOL_ID.to_string()) {
                                        let talk_resp = match das_node.overlay.process_one_request(&req).await {
                                            Ok(response) => discv5_overlay::portalnet::types::messages::Message::from(response).into(),
                                            Err(err) => {
                                                // if matches!(err, OverlayRequestError::UtpError(..)) {
                                                //     return;
                                                // }

                                                error!("Node {chan} Error processing request: {}", err);
                                                return;
                                            },
                                        };

                                        if let Err(err) = req.respond(talk_resp) {
                                            error!("Unable to respond to talk request: {}", err);
                                            return;
                                        }

                                        return;
                                    }

                                    let resp = handle_talk_request(req.node_id().clone(), req.protocol(), req.body().to_vec(), das_node, opts, enr_to_libp2p, node_ids, i).await;
                                    req.respond(resp);
                                });
                            },
                            Discv5Event::FindValue(req) => {
                                debug!("Stream {}: FindValue request received with id {}", chan, req.id());
                                msg_counter.send(MsgCountCmd::Increment);
                                clone_all!(das_node, opts, enr_to_libp2p, node_ids);
                                tokio::spawn(async move {
                                    let resp = handle_sampling_request(req.node_id().clone(), req.key(), &das_node, &opts).await;
                                    req.respond(resp);
                                });
                            },
                        }
                    },
                    Some(crate::libp2p::TalkReqMsg{resp_tx, peer_id, payload, protocol}) = libp2p_msgs.next() => {
                        debug!("Libp2p {i}: Talk request received");
                        msg_counter.send(MsgCountCmd::Increment);
                        let from = libp2p_to_enr.read().await.get(&peer_id).unwrap().clone();
                        clone_all!(das_node, opts, enr_to_libp2p, node_ids);
                        tokio::spawn(async move {
                            resp_tx.send(Ok(handle_talk_request(from, &protocol, payload, das_node, opts, enr_to_libp2p, node_ids, i).await));
                        });
                    },
                    Some(command) = overlay_service.command_rx.recv() => {
                        match command {
                            OverlayCommand::Request(request) => overlay_service.process_request(request),
                            OverlayCommand::FindContentQuery { target, callback } => {
                                if let Some(query_id) = overlay_service.init_find_content_query(target.clone(), Some(callback)) {
                                    debug!(
                                        query_id=query_id.to_string(),
                                        content_id=hex_encode_compact(target.content_id()),
                                        "FindContent query initialized"
                                    );
                                }
                            }
                        }
                    }
                    Some(response) = overlay_service.response_rx.recv() => {
                        // Look up active request that corresponds to the response.
                        let optional_active_request = overlay_service.active_outgoing_requests.write().remove(&response.request_id);
                        if let Some(active_request) = optional_active_request {

                            // Send response to responder if present.
                            if let Some(responder) = active_request.responder {
                                let _ = responder.send(response.response.clone());
                            }

                            // Perform background processing.
                            match response.response {
                                Ok(response) => overlay_service.process_response(response, active_request.destination, active_request.request, active_request.query_id),
                                Err(error) => overlay_service.process_request_failure(response.request_id, active_request.destination, error),
                            }

                        } else {
                            warn!("No request found for response");
                        }
                    }
                    Some(Ok(node_id)) = overlay_service.peers_to_ping.next() => {
                        // If the node is in the routing table, then ping and re-queue the node.
                        let key = discv5::kbucket::Key::from(node_id);
                        if let discv5::kbucket::Entry::Present(ref mut entry, _) = overlay_service.kbuckets.write().entry(&key) {
                            overlay_service.ping_node(&entry.value().enr());
                            overlay_service.peers_to_ping.insert(node_id);
                        }
                    }
                    // todo: uncommenting next clause will servilely affect performance :/ investigate why
                    // query_event = OverlayService::<DASContentKey, XorMetric, DASValidator, MemoryContentStore>::query_event_poll(&mut overlay_service.find_node_query_pool) => {
                    //     overlay_service.handle_find_nodes_query_event(query_event);
                    // }
                    // Handle query events for queries in the find content query pool.
                    query_event = OverlayService::<DASContentKey, XorMetric, DASValidator, MemoryContentStore>::query_event_poll(&mut overlay_service.find_content_query_pool) => {
                        overlay_service.handle_find_content_query_event(query_event);
                    }
                    _ = OverlayService::<DASContentKey, XorMetric, DASValidator, MemoryContentStore>::bucket_maintenance_poll(overlay_service.protocol.clone(), &overlay_service.kbuckets) => {}
                    _ = bucket_refresh_interval.tick() => {
                        overlay_service.bucket_refresh_lookup();
                    }
                    Some(event) = utp_listener_rx.recv() => das_node.overlay.process_utp_event(event).unwrap(),
                }
            }
        });
    }
    let enrs_stats = enrs.clone();
    let stats_task = tokio::spawn(async move {
        let enrs = enrs_stats;

        play_simulation(
            &options,
            &das_nodes,
            enr_to_libp2p.clone(),
            node_ids,
            msg_counter,
        )
        .await;
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

    let snapshot_dir = match &*opts.snapshot {
        "new" => {
            let snap_time: DateTime<Utc> = SystemTime::now().into();
            let snap_dir =
                PathBuf::from(&opts.cache_dir).join(snap_time.format("%Y-%m-%d-%T").to_string());
            fs::create_dir_all(&snap_dir).unwrap();
            snap_dir
        }
        "last" => {
            let mut paths: Vec<_> = fs::read_dir(&opts.cache_dir)
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            paths.sort_by_key(|dir| dir.metadata().unwrap().created().unwrap());
            paths.last().unwrap().path()
        }
        snap => PathBuf::from(&opts.cache_dir).join(snap),
    };

    info!("snapshot: {}", snapshot_dir.to_str().unwrap());

    for i in 0..node_count {
        let listen_addr = format!("{}:{}", listen_ip, port_start + i)
            .parse::<SocketAddr>()
            .unwrap();
        debug!("{}", listen_addr);

        let enr_key = match &*opts.snapshot {
            "new" => {
                let key = CombinedKey::generate_secp256k1();
                fs::write(snapshot_dir.join(format!("{i}.pem")), key.encode()).unwrap();
                key
            }
            _ => {
                let mut key_bytes = fs::read(snapshot_dir.join(format!("{i}.pem"))).unwrap();
                CombinedKey::secp256k1_from_bytes(&mut key_bytes).unwrap()
            }
        };

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
        debug!("Node Id: {}", enr.node_id());
        if enr.udp4_socket().is_some() {
            debug!("Base64 ENR: {}", enr.to_base64());
            debug!(
                "IP: {}, UDP_PORT:{}",
                enr.ip4().unwrap(),
                enr.udp4().unwrap()
            );
        } else {
            warn!("ENR is not printed as no IP:PORT was specified");
        }

        // default configuration
        let mut config_builder = Discv5ConfigBuilder::default();
        config_builder.request_retries(10);
        config_builder.filter_max_nodes_per_ip(None);
        config_builder.request_timeout(Duration::from_secs(opts.request_timeout));
        config_builder.query_timeout(Duration::from_secs(60));

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
            let topology_seed = {
                let f = get_snapshot_file(&opts, "topology_seed");
                let seed = fs::read(&f).map_or(rand::thread_rng().gen::<u64>(), |b| {
                    b.as_slice().read_u64::<BigEndian>().unwrap()
                });
                fs::write(&f, seed.to_be_bytes()).unwrap();
                seed
            };

            let mut rng = StdRng::seed_from_u64(topology_seed);
            for (i, s) in discv5_servers.iter().enumerate() {
                let mut n = 150;
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

    discv5_servers
}

pub async fn play_simulation(
    opts: &Options,
    nodes: &Vec<DASNode>,
    addr_book: Arc<RwLock<HashMap<NodeId, (PeerId, Multiaddr)>>>,
    node_ids: Vec<NodeId>,
    msg_counter: mpsc::UnboundedSender<MsgCountCmd>,
) {
    match &opts.simulation_case {
        SimulationCase::Disseminate(args) => {
            let keys = (0..args.number_of_samples).map(|i| {
                let mut h = Keccak256::new();
                h.update(&i.to_be_bytes());
                NodeId::new(&h.finalize().try_into().unwrap())
            });

            let (keys_per_node, nodes_per_key) = disseminate_samples(
                keys.clone(),
                opts,
                &args,
                nodes,
                addr_book.clone(),
                &node_ids,
            )
            .await;

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
                .for_each(|(k, nodes)| debug!("key={} nodes={}", k.to_string(), nodes.len()));
            debug!("Keys total: {}", nodes_per_key.len());

            for k in keys {
                if !nodes_per_key.contains_key(&k) {
                    warn!("missing key: {}", k.to_string());
                }
            }

            let mut keys_stored_total = 0usize;
            keys_per_node
                .iter()
                .for_each(|(n, keys)| keys_stored_total += *keys);
            info!("total keys stored = {keys_stored_total} (storage overhead)");

            let unique_keys_stored = nodes_per_key.len();
            assert_eq!(unique_keys_stored, args.number_of_samples);

            let msg_count_total = {
                let (tx, rx) = oneshot::channel();
                msg_counter.send(MsgCountCmd::Get(tx)).unwrap();
                rx.await.unwrap()
            };
            info!("total messages sent = {msg_count_total} (communication overhead)");
        }
        SimulationCase::Sample(ref args) => {
            let keys = (0..args.dissemination_args.number_of_samples)
                .map(|i| {
                    let mut h = Keccak256::new();
                    h.update(&i.to_be_bytes());
                    NodeId::new(&h.finalize().try_into().unwrap())
                })
                .collect::<Vec<_>>();

            let (keys_per_node, nodes_per_key) = disseminate_samples(
                keys.clone().into_iter(),
                opts,
                &args.dissemination_args,
                nodes,
                addr_book.clone(),
                &node_ids,
            )
            .await;

            let nodes_by_node: HashMap<_, _> = nodes
                .iter()
                .map(|e| {
                    let node_id = e.discovery.discv5.local_enr().node_id();
                    let known_by = nodes
                        .iter()
                        .filter_map(|n| {
                            n.discovery
                                .discv5
                                .find_enr(&node_id)
                                .map(|x| n.discovery.discv5.local_enr().node_id())
                        })
                        .collect_vec();
                    (node_id, known_by)
                })
                .into_group_map()
                .into_iter()
                .map(|(n, ns)| {
                    let x = ns.into_iter().flatten().collect_vec();
                    (n, x)
                })
                .collect();

            let overlay_nodes_by_node: HashMap<_, _> = nodes
                .iter()
                .map(|e| {
                    let node_id = e.discovery.discv5.local_enr().node_id();
                    let known_by = nodes
                        .iter()
                        .filter_map(|n| {
                            let key = kbucket::Key::from(n.discovery.discv5.local_enr().node_id());
                            if let kbucket::Entry::Present(entry, _) =
                                e.overlay.kbuckets.write().entry(&key)
                            {
                                return Some(n.discovery.discv5.local_enr().node_id());
                            }
                            None
                        })
                        .collect_vec();
                    (node_id, known_by)
                })
                .into_group_map()
                .into_iter()
                .map(|(n, ns)| {
                    let x = ns.into_iter().flatten().collect_vec();
                    (n, x)
                })
                .collect();

            let mut keys_stored_total = 0usize;
            keys_per_node
                .iter()
                .for_each(|(n, keys)| keys_stored_total += *keys);
            info!("total keys stored = {keys_stored_total} (storage overhead)");

            let unique_keys_stored = nodes_per_key.len();
            assert_eq!(
                unique_keys_stored,
                args.dissemination_args.number_of_samples
            );

            let msg_count_total = {
                let (tx, rx) = oneshot::channel();
                msg_counter.send(MsgCountCmd::Get(tx)).unwrap();
                rx.await.unwrap()
            };
            info!("total messages sent = {msg_count_total} (communication overhead)");

            msg_counter.send(MsgCountCmd::Reset).unwrap();

            let validators_seed = {
                let f = get_snapshot_file(&opts, "validators_seed");
                let seed = fs::read(&f).map_or(rand::thread_rng().gen::<u64>(), |b| {
                    b.as_slice().read_u64::<BigEndian>().unwrap()
                });
                fs::write(&f, seed.to_be_bytes()).unwrap();
                seed
            };
            let mut rng = StdRng::seed_from_u64(validators_seed);

            let validators =
                rand::seq::index::sample(&mut rng, nodes.len(), args.validators_number)
                    .iter()
                    .map(|i| nodes[i].clone())
                    .collect::<Vec<_>>();

            let mut futures = vec![];

            for (i, validator) in validators.into_iter().enumerate() {
                let validator_node_id = validator.discovery.local_enr().node_id();
                let samples = rand::seq::index::sample(
                    &mut thread_rng(),
                    keys.len(),
                    args.samples_per_validator,
                )
                .iter()
                .map(|i| (i, keys[i]))
                .collect_vec();

                let parallelism = args.parallelism;
                let lookup_method = args.lookup_method.clone();

                clone_all!(
                    node_ids,
                    nodes_per_key,
                    nodes_by_node,
                    overlay_nodes_by_node
                );

                futures.push(async move {
                    let mut futures = FuturesUnordered::new();
                    let mut samples = samples;
                    let mut num_waiting = 0usize;
                    let mut num_success = 0usize;

                    loop {
                        if num_waiting < parallelism {
                            if let Some((j, sample_key)) = samples.pop() {
                                num_waiting += 1;
                                clone_all!(validator, node_ids, nodes_per_key, lookup_method, nodes_by_node, overlay_nodes_by_node);
                                futures.push(async move {
                                    if validator.samples.read().await.contains_key(&sample_key) {
                                        return Some((j, sample_key.clone(), b"yep".to_vec()))
                                    }

                                    info!("validator {i}: looking for a sample with key {}", NodeId::new(&DASContentKey::Sample(sample_key.raw()).content_id()).to_string());

                                    match lookup_method {
                                        LookupMethod::Discv5FindValue => match validator.discovery.discv5.find_value(sample_key).await {
                                            Ok(res) => Some((j, sample_key.clone(), res)),
                                            Err(e) => match e {
                                                FindValueError::RequestError(e) => {
                                                    error!("node {i} ({validator_node_id}) fail requesting sample {j} ({sample_key}): {e}");
                                                    None
                                                }
                                                FindValueError::RequestErrorWithEnrs((re, found_enrs)) => {
                                                    error!("node {i} ({validator_node_id}) fail requesting sample {j} ({sample_key}): {re}");

                                                    let host_nodes = nodes_per_key.get(&sample_key).unwrap().clone();
                                                    let local_info = host_nodes.iter().map(|e| (e.to_string(), Key::from(e.clone()).log2_distance(&Key::from(sample_key.clone())).unwrap())).sorted_by_key(|(x, y)| *y).collect_vec();
                                                    let search_info = found_enrs.iter().map(|e| (e.node_id().to_string(), Key::from(e.node_id().clone()).log2_distance(&Key::from(sample_key.clone())).unwrap())).sorted_by_key(|(x, y)| *y).collect_vec();
                                                    info!("missing sample is stored in {:?}, visited nodes: {:?}", local_info, search_info);
                                                    host_nodes.into_iter().for_each(|n| {
                                                        let info = nodes_by_node.get(&n).map(|x| x.into_iter().map(|e| (e.to_string(), Key::from(e.clone()).log2_distance(&Key::from(sample_key.clone())).unwrap())).sorted_by_key(|(x, y)| *y).collect_vec());
                                                        info!("node {} that store missing samples are stored in ({:?}) {:?}", n.to_string(), info.as_ref().map(|x| x.len()), info)
                                                    });
                                                    None
                                                }
                                            }
                                        }
                                        LookupMethod::OverlayFindContent => match validator.overlay.lookup_content(DASContentKey::Sample(sample_key.raw()))
                                            .await {
                                            Ok(res) => Some((j, sample_key.clone(), res)),
                                            Err(closest_nodes) => {
                                                error!("node {i} ({validator_node_id}) fail requesting sample {j} ({sample_key})");

                                                let host_nodes = nodes_per_key.get(&sample_key).unwrap().clone();
                                                let local_info = host_nodes.iter().map(|e| (e.to_string(), XorMetric::distance(&DASContentKey::Sample(Key::from(sample_key.clone()).hash.into()).content_id(), &e.raw()).log2())).collect_vec();
                                                let search_info = closest_nodes.iter().map(|e| (e.to_string(), XorMetric::distance(&DASContentKey::Sample(Key::from(sample_key.clone()).hash.into()).content_id(), &e.raw()).log2().unwrap())).sorted_by_key(|(x, y)| *y).collect_vec();
                                                info!("missing sample is stored in {:?}, visited nodes ({}): {:?}", local_info, search_info.len(), search_info);
                                                host_nodes.into_iter().for_each(|n| {
                                                    let info = nodes_by_node.get(&n).map(|x| x.into_iter().map(|e| (e.to_string(), XorMetric::distance(&DASContentKey::Sample(Key::from(sample_key.clone()).hash.into()).content_id(), &e.raw()).log2().unwrap())).sorted_by_key(|(x, y)| *y).collect_vec());
                                                    info!("node {} that store missing samples are stored in ({:?}) {:?}", n.to_string(), info.as_ref().map(|x| x.len()), info)
                                                });
                                                None
                                            }
                                        }
                                    }

                                });
                            }
                        }

                        if let Some(res) = futures.next().await {
                            num_waiting -= 1;
                            if let Some((j, sample_key, value)) = res {
                                debug!("[validator {i} ({validator_node_id})] success requesting sample {j} ({sample_key}): value='{}'", std::str::from_utf8(&value).unwrap());

                                num_success += 1;
                            }
                        }

                        if num_waiting == 0 && samples.is_empty() {
                            return num_success;
                        }
                    }
                });
            }

            futures::future::join_all(futures)
                .instrument(info_span!("random-sampling"))
                .await
                .into_iter()
                .enumerate()
                .for_each(|(i, num_success)| {
                    info!(
                        "validator {i}: samples found {num_success}/{}",
                        args.samples_per_validator
                    );
                });

            let msg_count_total = {
                let (tx, rx) = oneshot::channel();
                msg_counter.send(MsgCountCmd::Get(tx)).unwrap();
                rx.await.unwrap()
            };
            info!("total messages sent = {msg_count_total} (communication overhead)");
        }
        _ => {}
    }
}

pub async fn handle_talk_request(
    from: NodeId,
    protocol: &[u8],
    message: Vec<u8>,
    node: DASNode,
    opts: Options,
    addr_book: Arc<RwLock<HashMap<NodeId, (PeerId, Multiaddr)>>>,
    node_ids: Vec<NodeId>,
    node_idx: usize,
) -> Vec<u8> {
    match protocol {
        DISSEMINATION_PROTOCOL_ID => match opts.simulation_case {
            SimulationCase::Disseminate(ref args) => handle_dissemination_request(
                from, message, node, opts.clone(), args.clone(), addr_book, node_ids, node_idx,
            ),
            SimulationCase::Sample(ref args) => handle_dissemination_request(
                from,
                message,
                node,
                opts.clone(),
                args.dissemination_args.clone(),
                addr_book,
                node_ids,
                node_idx,
            ),
        }
        _ => panic!("unexpected protocol_id"),
    }
}


async fn disseminate_samples(
    keys: impl Iterator<Item = NodeId>,
    opts: &Options,
    args: &DisseminationArgs,
    nodes: &Vec<DASNode>,
    addr_book: Arc<RwLock<HashMap<NodeId, (PeerId, Multiaddr)>>>,
    node_ids: &Vec<NodeId>,
) -> (HashMap<NodeId, usize>, HashMap<NodeId, Vec<NodeId>>) {
    let node = nodes[0].clone();
    let local_node_id = node.discovery.discv5.local_enr().node_id();

    let alloc = match args.batching_strategy {
        BatchingStrategy::BucketWise => {
            let local_view: HashMap<_, _> = node
                .discovery
                .discv5
                .kbuckets()
                .buckets_iter()
                .map(|kb| {
                    kb.iter()
                        .map(|e| e.key.preimage().clone())
                        .collect::<Vec<_>>()
                })
                .enumerate()
                .collect();

            keys.into_iter()
                .flat_map(|k| {
                    let i =
                        BucketIndex::new(&Key::from(local_node_id.clone()).distance(&Key::from(k)))
                            .unwrap()
                            .get();
                    let local_nodes = local_view.get(&i).unwrap().clone();
                    /// if **replicate-all* then a receiver node applies forwards samples to more then one node in every k-bucket it handles
                    let contacts_in_bucket = local_nodes.into_iter();
                    let mut forward_to: Vec<_> = match args.replicate_mode {
                        ReplicatePolicy::ReplicateOne => contacts_in_bucket.take(1).collect(),
                        ReplicatePolicy::ReplicateSome => {
                            contacts_in_bucket.take(1 + &args.redundancy).collect()
                        }
                        ReplicatePolicy::ReplicateAll => contacts_in_bucket.collect(),
                    };

                    if forward_to.is_empty() {
                        forward_to.push(local_node_id);
                    }

                    forward_to
                        .into_iter()
                        .map(|n| (n, Key::from(k)))
                        .collect::<Vec<_>>()
                })
                .into_group_map()
        }
        BatchingStrategy::DistanceWise => {
            let mut local_view = node
                .discovery
                .discv5
                .kbuckets()
                .buckets_iter()
                .flat_map(|kb| {
                    kb.iter()
                        .map(|e| e.key.preimage().clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            local_view.push(local_node_id);

            keys.into_iter()
                .flat_map(|k| {
                    /// if **replicate-all* then a receiver node applies forwards samples to more then one node in every k-bucket it handles
                    let contacts_in_bucket = local_view
                        .clone()
                        .into_iter()
                        .sorted_by_key(|n| Key::from(n.clone()).distance(&Key::from(k)));
                    let mut forward_to: Vec<_> = match args.replicate_mode {
                        ReplicatePolicy::ReplicateOne => contacts_in_bucket.take(1).collect(),
                        ReplicatePolicy::ReplicateSome => {
                            contacts_in_bucket.take(1 + &args.redundancy).collect()
                        }
                        ReplicatePolicy::ReplicateAll => contacts_in_bucket.collect(),
                    };

                    if forward_to.is_empty() {
                        forward_to.push(local_node_id);
                    }

                    forward_to
                        .into_iter()
                        .map(|n| (n, Key::from(k)))
                        .collect::<Vec<_>>()
                })
                .into_group_map()
        }
    };



    let mut futures = vec![];
    for (next, mut keys) in alloc.into_iter() {
        if next == local_node_id {
            warn!("no peers to forward {} keys to, saved locally", keys.len());

            let mut samples = node.samples.write().await;
            keys.clone()
                .into_iter()
                .for_each(|k| match samples.entry(k.preimage().clone()) {
                    Entry::Occupied(mut e) => e.get_mut().add_assign(1),
                    Entry::Vacant(mut e) => {
                        e.insert(1);
                    }
                });
            continue;
        }

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

        let node = nodes[0].clone();
        let enr = node.discovery.find_enr(&next).unwrap();
        let addr_book = addr_book.clone();

        {
            let next_i = node_ids.iter().position(|e| *e == next).unwrap();
            debug!(
                "node {0} ({}) sends {} keys for request (id={}) to {next_i} ({})",
                node.discovery.local_enr().node_id(),
                keys.len(),
                hex::encode(&batch_id),
                next
            );
        }

        clone_all!(msg, keys);
        futures.push(Box::pin(async move {
            match opts.wire_protocol {
                TalkWire::Discv5 => {
                    // let mut i = 0;
                    // loop {
                    //     let id = &*nanoid!(8).into_bytes();
                    //     let msg = {
                    //         let mut m = vec![];
                    //         let mut w = BufWriter::new(&mut *m);
                    //         w.write(id.clone()).unwrap();
                    //         keys.iter().skip(i * 28).take(28).for_each(|k| {
                    //             let _ = w.write(&k.hash.to_vec());
                    //         });
                    //         w.buffer().to_vec()
                    //     };
                    //
                    //     if msg.len() == 8 {
                    //         break;
                    //     }
                    //
                    //     i += 1;
                    //     let _ = node
                    //         .discovery
                    //         .discv5
                    //         .talk_req(enr.clone(), DISSEMINATION_PROTOCOL_ID.to_vec(), msg)
                    //         .await
                    //         .map_err(|e| eyre::eyre!("{e}"));
                    // }

                    let resp_bytes = node.overlay.send_elastic_talk_req(
                        enr.clone(), DISSEMINATION_PROTOCOL_ID.to_vec(), msg
                    ).await.unwrap();
                }
                TalkWire::Libp2p => {
                    let (peer_id, addr) =
                        addr_book.read().await.get(&enr.node_id()).unwrap().clone();
                    let _ = node
                        .libp2p
                        .talk_req(&peer_id, &addr, DISSEMINATION_PROTOCOL_ID, msg)
                        .await
                        .unwrap();
                }
            }
        }));
    }
    futures::future::join_all(futures)
        .instrument(info_span!("bucket-cast"))
        .await;

    let mut keys_per_node = HashMap::new();
    let mut nodes_per_key = HashMap::<_, Vec<NodeId>>::new();

    for n in nodes {
        let samples = n.samples.read().await;
        samples.keys().for_each(|k| {
            keys_per_node.insert(n.discovery.local_enr().node_id(), samples.len());

            nodes_per_key
                .entry(k.clone())
                .and_modify(|e| e.push(n.discovery.local_enr().node_id()))
                .or_insert(vec![n.discovery.local_enr().node_id()]);
        })
    }

    return (keys_per_node, nodes_per_key);
}

fn handle_dissemination_request(
    from: NodeId,
    message: Vec<u8>,
    node: DASNode,
    opts: Options,
    args: DisseminationArgs,
    addr_book: Arc<RwLock<HashMap<NodeId, (PeerId, Multiaddr)>>>,
    node_ids: Vec<NodeId>,
    node_idx: usize,
) -> Vec<u8> {
    let promise_id = utp::stream::rand();

    tokio::spawn(async move {
        let message = match opts.wire_protocol {
            TalkWire::Discv5 => {
                let content: ElasticPacket = ElasticPacket::from_ssz_bytes(&*message).unwrap();
                match content {
                    ElasticPacket::Data(bytes) => bytes.to_vec(),
                    ElasticPacket::ConnectionId(conn_id) => {
                        let conn_id = u16::from_be(conn_id);
                        let enr = node.discovery.find_enr_or_cache(&from).unwrap();
                        node.overlay.init_find_content_stream(enr, conn_id).await.unwrap()
                    }
                    ElasticPacket::Result((promise_id, res)) => {
                        match opts.wire_protocol {
                            TalkWire::Discv5 => {
                                node.overlay.handle_promise_result(promise_id, res).await
                            },
                            TalkWire::Libp2p => node.active_requests.write()
                                .await
                                .remove(&promise_id).expect("receive result for an unknown promise")
                                .send(res).unwrap()
                        }

                        return;
                    }
                    _ => unreachable!()
                }
            }
            TalkWire::Libp2p => message
        };

        let local_node_id = node.discovery.local_enr().node_id();

        let from_i = node_ids.iter().position(|e| *e == from).unwrap();

        let mut r = BufReader::new(&*message);
        let mut keys = vec![];

        let mut id = [0; 8];
        r.read(&mut id).unwrap();
        let id = id.to_vec();

        {
            debug!(
            "node {node_idx} ({}) attempts to get lock for request (id={}) from {from_i} ({})",
            node.discovery.local_enr().node_id(),
            hex::encode(&id),
            from
        );
            let mut handled_ids = node.handled_ids.write().await;
            if handled_ids.contains_key(&id) && args.forward_mode != ForwardPolicy::ForwardAll {
                debug!(
                    "node {node_idx} ({}) skipped request (id={}) from {from_i} ({})",
                    node.discovery.local_enr().node_id(),
                    hex::encode(&id),
                    from
                );

                let enr = node.discovery.find_enr_or_cache(&from).unwrap();
                node.discovery.discv5.talk_req(enr, DISSEMINATION_PROTOCOL_ID.to_vec(), ElasticPacket::Result((promise_id, vec![])).as_ssz_bytes());
                return;
            } else {
                debug!(
                "node {node_idx} ({}) received request (id={}) from {from_i} ({})",
                node.discovery.local_enr().node_id(),
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

        // debug!("node {node_idx} ({}) receives {:?} keys for request (id={}) from {from_i} ({})", node.discv5.local_enr().node_id(), keys.iter().map(|e| e.to_string()).collect_vec(), hex::encode(&id), from);

        let alloc = match args.batching_strategy {
            BatchingStrategy::BucketWise => {
                let local_view: HashMap<_, _> = node
                    .discovery
                    .discv5
                    .kbuckets()
                    .buckets_iter()
                    .map(|kb| kb.iter().map(|e| e.key.preimage().clone()).collect_vec())
                    .enumerate()
                    .collect();

                keys.into_iter()
                    .flat_map(|k| {
                        let i =
                            BucketIndex::new(&Key::from(local_node_id.clone()).distance(&Key::from(k)))
                                .unwrap()
                                .get();
                        let local_nodes = local_view.get(&i).unwrap().clone();
                        /// if **replicate-all* then a receiver node applies forwards samples to more then one node in every k-bucket it handles
                        let contacts_in_bucket = local_nodes.into_iter().filter(|e| *e != from);
                        let mut forward_to: Vec<_> = match args.replicate_mode {
                            ReplicatePolicy::ReplicateOne => contacts_in_bucket.take(1).collect(),
                            ReplicatePolicy::ReplicateSome => {
                                contacts_in_bucket.take(1 + &args.redundancy).collect()
                            }
                            ReplicatePolicy::ReplicateAll => contacts_in_bucket.collect(),
                        };

                        if forward_to.is_empty() {
                            forward_to.push(local_node_id);
                        }

                        forward_to
                            .into_iter()
                            .map(|n| (n, Key::from(k)))
                            .collect::<Vec<_>>()
                    })
                    .into_group_map()
            }
            BatchingStrategy::DistanceWise => {
                let mut local_view = node
                    .discovery
                    .discv5
                    .kbuckets()
                    .buckets_iter()
                    .flat_map(|kb| {
                        kb.iter()
                            .map(|e| e.key.preimage().clone())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                local_view.push(local_node_id.clone());

                keys.into_iter()
                    .flat_map(|k| {
                        let contacts_in_bucket = local_view
                            .clone()
                            .into_iter()
                            .filter(|e| *e != from)
                            .sorted_by_key(|n| Key::from(n.clone()).distance(&Key::from(k)));
                        let mut forward_to: Vec<_> = match args.replicate_mode {
                            ReplicatePolicy::ReplicateOne => contacts_in_bucket.take(1).collect(),
                            ReplicatePolicy::ReplicateSome => {
                                contacts_in_bucket.take(1 + &args.redundancy).collect()
                            }
                            ReplicatePolicy::ReplicateAll => contacts_in_bucket.collect(),
                        };

                        forward_to
                            .into_iter()
                            .map(|n| (n, Key::from(k)))
                            .collect::<Vec<_>>()
                    })
                    .into_group_map()
            }
        };

        let mut futures = FuturesUnordered::new();

        for (next, keys) in alloc.into_iter() {
            if next == local_node_id {
                let mut samples = node.samples.write().await;
                let mut store = node.overlay.store.write();
                keys.clone().into_iter().for_each(|k| {
                    store
                        .put(DASContentKey::Sample(k.preimage().raw()), b"yep".to_vec())
                        .unwrap();

                    match samples.entry(k.preimage().clone()) {
                        Entry::Occupied(mut e) => e.get_mut().add_assign(1),
                        Entry::Vacant(mut e) => {
                            e.insert(1);
                        }
                    }
                });

                continue;
            }

            let enr = node.discovery.find_enr(&next).unwrap();

            let next_i = node_ids.iter().position(|e| *e == next).unwrap();
            debug!(
            "node {node_idx} ({}) sends {:?} keys for request (id={}) to {next_i} ({})",
            node.discovery.local_enr().node_id(),
            keys.iter()
                .map(|e| e.preimage().to_string())
                .collect::<Vec<_>>(),
            hex::encode(&id),
            next
        );

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
                    match opts.wire_protocol {
                        TalkWire::Discv5 => {
                            // node
                            //     .discovery
                            //     .discv5
                            //     .talk_req(enr, DISSEMINATION_PROTOCOL_ID.to_vec(), msg)
                            //     .await
                            //     .map_err(|e| eyre::eyre!("{e}"))
                            node.overlay.send_elastic_talk_req(
                                enr.clone(), DISSEMINATION_PROTOCOL_ID.to_vec(), msg
                            ).await.map_err(|e| eyre::eyre!("{e}"))
                            // Ok(vec![])
                        },
                        TalkWire::Libp2p => {
                            let (peer_id, addr) =
                                addr_book.read().await.get(&enr.node_id()).unwrap().clone();
                            node.libp2p
                                .talk_req(&peer_id, &addr, DISSEMINATION_PROTOCOL_ID, msg)
                                .await
                        }
                    }
                        .map_err(|e| eyre::eyre!("error making request (id={}) from {} to {}: {e}",
                            hex::encode(&id),
                            node.discovery.local_enr().node_id(),
                            enr.node_id(),
                        ))
                });
            }
        }

        while let Some(resp) = futures.next().await {
            resp.unwrap();
        }

        let enr = node.discovery.find_enr_or_cache(&from).unwrap();
        node.discovery.discv5.talk_req(enr, DISSEMINATION_PROTOCOL_ID.to_vec(), ElasticPacket::Result((promise_id, vec![])).as_ssz_bytes()).await.unwrap();
    });

    ElasticPacket::Promise(promise_id).as_ssz_bytes()
}

async fn handle_sampling_request(
    _from: NodeId,
    key: &NodeId,
    node: &DASNode,
    opts: &Options,
) -> Option<Vec<u8>> {
    let mut samples = node.samples.read().await;

    debug!("receive sampling request, have {} samples total, distance to requested key={:?}, have requested key = {}", samples.len(), Key::from(node.discovery.discv5.local_enr().node_id()).log2_distance(&Key::from(key.clone())), samples.contains_key(key));

    samples.get(key).map(|e| b"yep".to_vec())
}

fn get_snapshot_file<S: AsRef<str>>(opts: &Options, file: S) -> PathBuf {
    match &*opts.snapshot {
        "new" | "last" => {
            let mut paths: Vec<_> = fs::read_dir(&opts.cache_dir)
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            paths.sort_by_key(|dir| dir.metadata().unwrap().created().unwrap());
            paths.last().unwrap().path().join(file.as_ref())
        }
        snap => PathBuf::from(&opts.cache_dir)
            .join(snap)
            .join(file.as_ref()),
    }
}
