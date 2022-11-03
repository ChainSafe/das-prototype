use discv5::Enr;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use libp2p::core::transport::upgrade;
use libp2p::identity::Keypair;
use libp2p::noise::NoiseConfig;
use libp2p::request_response::{
    ProtocolSupport, RequestId, RequestResponse, RequestResponseCodec, RequestResponseConfig,
    RequestResponseEvent, RequestResponseMessage, ResponseChannel,
};
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
    SwarmEvent,
};
use libp2p::tcp::TcpConfig;
use libp2p::NetworkBehaviour;
use libp2p::{identity, mplex, noise, Multiaddr, PeerId, Swarm, Transport};
use rocket::form::FromForm;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{io, iter};
use std::borrow::Cow;
use tokio::net::tcp;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tracing::{debug, info, log::warn};

const PROTOCOL_ID: &str = "/das/0.1.0";

/// The Libp2pService listens to events from the Libp2p swarm.
pub struct Libp2pDaemon {
    swarm: Swarm<Behaviour>,
    from_service: UnboundedReceiver<NetworkMessage>,
    local_peer_id: PeerId,
    node_index: usize,
}

#[derive(Clone)]
pub struct Libp2pService {
    /// Local copy of the `PeerId` of the local node.
    pub(crate) local_peer_id: PeerId,
    /// Channel for sending requests to worker.
    to_worker: UnboundedSender<NetworkMessage>,
}

/// Messages into the service to handle.
#[derive(Debug)]
pub enum NetworkMessage {
    RequestResponse {
        peer_id: PeerId,
        addr: Multiaddr,
        protocol: Vec<u8>,
        payload: Vec<u8>,
        resp_tx: oneshot::Sender<eyre::Result<Vec<u8>>>,
    },
}

#[derive(Debug)]
pub struct TalkReqMsg {
    pub peer_id: PeerId,
    pub protocol: Vec<u8>,
    pub payload: Vec<u8>,
    pub resp_tx: oneshot::Sender<eyre::Result<Vec<u8>>>,
}

impl Libp2pDaemon {
    pub fn new(
        keypair: Keypair,
        addr: Multiaddr,
        node_index: usize,
    ) -> (
        Libp2pDaemon,
        mpsc::UnboundedReceiver<TalkReqMsg>,
        Libp2pService,
    ) {
        let local_peer_id = PeerId::from(keypair.public());

        let transport = {
            let dh_keys = noise::Keypair::<noise::X25519Spec>::new()
                .into_authentic(&keypair)
                .expect("Noise key generation failed");

            TcpConfig::new()
                .upgrade(upgrade::Version::V1)
                .authenticate(NoiseConfig::xx(dh_keys).into_authenticated())
                .multiplex(mplex::MplexConfig::new())
                .boxed()
        };

        let (message_sink, message_chan) = mpsc::unbounded_channel();
        let mut swarm = Swarm::new(
            transport,
            Behaviour::new(message_sink, node_index),
            local_peer_id,
        );

        // Listen on the addresses.
        if let Err(err) = swarm.listen_on(addr) {
            warn!(target: "sub-libp2p", "Can't listen on 'listen_address' because: {:?}", err)
        }

        let (network_sender_in, network_receiver_in) = mpsc::unbounded_channel();

        let worker = Libp2pDaemon {
            local_peer_id,
            swarm,
            from_service: network_receiver_in,
            node_index,
        };

        let service = Libp2pService {
            local_peer_id,
            to_worker: network_sender_in,
        };

        (worker, message_chan, service)
    }

    /// Starts the libp2p service networking stack.
    pub async fn run(mut self) {
        let mut swarm_stream = self.swarm.fuse();
        let mut network_stream = UnboundedReceiverStream::new(self.from_service);
        let libp2p_node_idx = self.node_index;

        loop {
            select! {
                swarm_event = swarm_stream.next() => match swarm_event {
                    // Outbound events
                    Some(event) => match event {
                        SwarmEvent::Behaviour(BehaviourEvent::InboundMessage{peer}) => {
                            debug!(libp2p_node_idx=libp2p_node_idx, "Inbound message from {:?}", peer);
                        },
                        SwarmEvent::NewListenAddr { address, .. } => debug!(libp2p_node_idx=libp2p_node_idx, "Listening on {:?}", address),
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            debug!(libp2p_node_idx=libp2p_node_idx, "ConnectionEstablished with {:?}", peer_id.to_string());
                        },
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            debug!(libp2p_node_idx=libp2p_node_idx, "ConnectionClosed with {:?}", peer_id.to_string());
                        }
                        _ => continue
                    }
                    None => { break; }
                },
                rpc_message = network_stream.next() => match rpc_message {
                    // Inbound requests
                    Some(request) => {
                        let behaviour = swarm_stream.get_mut().behaviour_mut();

                        match request {
                            NetworkMessage::RequestResponse {
                                peer_id,
                                addr,
                                protocol,
                                payload,
                                resp_tx,
                            } => {
                                behaviour.send_request(peer_id, addr, protocol, payload, resp_tx);
                            }
                        }
                    }
                    None => { break; }
                }
            };
        }
    }
}

impl Libp2pService {
    pub async fn talk_req(
        &self,
        peer_id: &PeerId,
        addr: &Multiaddr,
        protocol: &[u8],
        payload: Vec<u8>,
    ) -> eyre::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.to_worker
            .send(NetworkMessage::RequestResponse {
                peer_id: peer_id.clone(),
                addr: addr.clone(),
                protocol: protocol.to_vec(),
                payload,
                resp_tx: tx,
            })
            .map_err(|e| eyre::eyre!("{e}"))?;

        rx.await.unwrap()
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(
    out_event = "BehaviourEvent",
    poll_method = "poll",
    event_process = true
)]
pub(crate) struct Behaviour {
    req_resp: RequestResponse<GenericCodec>,

    #[behaviour(ignore)]
    events: VecDeque<BehaviourEvent>,

    #[behaviour(ignore)]
    message_sink: mpsc::UnboundedSender<TalkReqMsg>,

    #[behaviour(ignore)]
    pending_requests: HashMap<RequestId, Option<oneshot::Sender<eyre::Result<Vec<u8>>>>>,

    #[behaviour(ignore)]
    pending_responses:
        FuturesUnordered<Pin<Box<dyn Future<Output = Option<RequestProcessingOutcome>> + Send>>>,

    #[behaviour(ignore)]
    node_index: usize,
}

struct RequestProcessingOutcome {
    inner_channel: ResponseChannel<Result<Vec<u8>, ()>>,
    response: Result<Vec<u8>, ()>,
}

pub(crate) enum BehaviourEvent {
    InboundMessage {
        /// Peer which sent us a message.
        peer: PeerId,
    },
}

impl Behaviour {
    pub fn new(message_sink: mpsc::UnboundedSender<TalkReqMsg>, node_index: usize) -> Self {
        Self {
            req_resp: RequestResponse::new(
                GenericCodec {
                    max_request_size: 100000,
                    max_response_size: 100000,
                },
                iter::once((PROTOCOL_ID.as_bytes().to_vec(), ProtocolSupport::Full)),
                RequestResponseConfig::default(),
            ),
            events: Default::default(),
            message_sink,
            pending_requests: Default::default(),
            pending_responses: Default::default(),
            node_index,
        }
    }

    pub fn send_request(
        &mut self,
        peer_id: PeerId,
        addr: Multiaddr,
        protocol: Vec<u8>,
        payload: Vec<u8>,
        resp_tx: oneshot::Sender<eyre::Result<Vec<u8>>>,
    ) {
        self.req_resp.add_address(&peer_id, addr);
        let req_id = self.req_resp.send_request(&peer_id, TalkRequest{
            protocol,
            payload
        });
        self.pending_requests.insert(req_id, Some(resp_tx));
    }

    fn poll(
        &mut self,
        cx: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<BehaviourEvent, <Self as NetworkBehaviour>::ProtocolsHandler>>
    {
        let node_index = self.node_index;

        // Poll to see if any response is ready to be sent back.
        while let Poll::Ready(Some(outcome)) = self.pending_responses.poll_next_unpin(cx) {
            let RequestProcessingOutcome {
                inner_channel,
                response,
            } = match outcome {
                Some(outcome) => outcome,
                // The response builder was too busy and thus the request was dropped. This is
                // later on reported as a `InboundFailure::Omission`.
                None => break,
            };
            if let Err(_) = self.req_resp.send_response(inner_channel, response) {
                warn!("failed to send response");
            }
        }

        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
        }

        Poll::Pending
    }
}

impl NetworkBehaviourEventProcess<RequestResponseEvent<TalkRequest, Result<Vec<u8>, ()>>>
    for Behaviour
{
    fn inject_event(&mut self, event: RequestResponseEvent<TalkRequest, Result<Vec<u8>, ()>>) {
        let node_index = self.node_index;
        match event {
            RequestResponseEvent::Message { peer, message } => {
                match message {
                    RequestResponseMessage::Request {
                        request, channel, ..
                    } => {
                        let (tx, rx) = oneshot::channel();
                        self.message_sink
                            .send(TalkReqMsg {
                                peer_id: peer,
                                protocol: request.protocol,
                                payload: request.payload,
                                resp_tx: tx,
                            })
                            .unwrap();

                        self.pending_responses.push(Box::pin(async move {
                            rx.await
                                .map(|response| RequestProcessingOutcome {
                                    inner_channel: channel,
                                    response: response.map_err(|_| ()),
                                })
                                .ok()
                        }));
                    }
                    RequestResponseMessage::Response {
                        request_id,
                        response,
                    } => match self.pending_requests.entry(request_id) {
                        Entry::Occupied(e) => {
                            if let Some(tx) = e.remove() {
                                tx.send(response.map_err(|_| eyre::eyre!("got error resp")))
                                    .unwrap();
                            }
                        }
                        Entry::Vacant(_) => panic!("unknown request_id"),
                    },
                }
                self.events
                    .push_back(BehaviourEvent::InboundMessage { peer });
            }
            RequestResponseEvent::OutboundFailure { peer, error, .. } => {
                debug!(
                    libp2p_node_idx = node_index,
                    "OutboundFailure {:?}: {}", peer, error
                );
            }
            RequestResponseEvent::InboundFailure { peer, error, .. } => {
                debug!(
                    libp2p_node_idx = node_index,
                    "InboundFailure {:?}: {}", peer, error
                );
            }
            RequestResponseEvent::ResponseSent { .. } => {}
        }
    }
}

#[derive(Clone, Debug)]
pub struct TalkRequest {
    pub protocol: Vec<u8>,
    pub payload: Vec<u8>
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct GenericCodec {
    pub max_request_size: u64,
    pub max_response_size: u64,
}

#[async_trait::async_trait]
impl RequestResponseCodec for GenericCodec {
    type Protocol = Vec<u8>;
    type Request = TalkRequest;
    type Response = Result<Vec<u8>, ()>;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        mut io: &mut T,
    ) -> io::Result<Self::Request>
        where
            T: AsyncRead + Unpin + Send,
    {
        let protocol_length = unsigned_varint::aio::read_usize(&mut io)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

        let mut protocol = vec![0; protocol_length];
        io.read_exact(&mut protocol).await?;

        // Read the length.
        let payload_length = unsigned_varint::aio::read_usize(&mut io)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

        if payload_length > usize::try_from(self.max_request_size).unwrap_or(usize::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Request size exceeds limit: {} > {}",
                    payload_length, self.max_request_size
                ),
            ));
        }

        // Read the payload.
        let mut payload = vec![0; payload_length];
        io.read_exact(&mut payload).await?;

        Ok(TalkRequest{
            protocol,
            payload
        })
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        mut io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // Read the length.
        let length = match unsigned_varint::aio::read_usize(&mut io).await {
            Ok(l) => l,
            Err(unsigned_varint::io::ReadError::Io(err))
                if matches!(err.kind(), io::ErrorKind::UnexpectedEof) =>
            {
                return Ok(Err(()));
            }
            Err(err) => return Err(io::Error::new(io::ErrorKind::InvalidInput, err)),
        };

        if length > usize::try_from(self.max_response_size).unwrap_or(usize::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Response size exceeds limit: {} > {}",
                    length, self.max_response_size
                ),
            ));
        }

        // Read the payload.
        let mut buffer = vec![0; length];
        io.read_exact(&mut buffer).await?;
        Ok(Ok(buffer))
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // Write the protocol_length.
        {
            let mut buffer = unsigned_varint::encode::usize_buffer();
            io.write_all(unsigned_varint::encode::usize(req.protocol.len(), &mut buffer))
                .await?;
        }

        // Write protocol.
        io.write_all(&req.protocol).await?;

        // Write the payload_length.
        {
            let mut buffer = unsigned_varint::encode::usize_buffer();
            io.write_all(unsigned_varint::encode::usize(req.payload.len(), &mut buffer))
                .await?;
        }

        // Write the payload.
        io.write_all(&req.payload).await?;

        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // If `res` is an `Err`, we jump to closing the substream without writing anything on it.
        if let Ok(res) = res {
            let mut buffer = unsigned_varint::encode::usize_buffer();
            io.write_all(unsigned_varint::encode::usize(res.len(), &mut buffer))
                .await?;

            // Write the payload.
            io.write_all(&res).await?;
        }

        io.close().await?;
        Ok(())
    }
}
