use std::{
    collections::{HashMap, HashSet, VecDeque},
    task::{Context, Poll, Waker},
};

use either::Either;
use futures::{
    future::BoxFuture,
    io::{ReadHalf, WriteHalf},
    stream::{BoxStream, FuturesUnordered},
    AsyncReadExt, AsyncWriteExt, FutureExt, StreamExt,
};
use indexmap::IndexSet;
use libp2p::{
    core::{transport::PortUse, Endpoint},
    swarm::{
        ConnectionClosed, ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler,
        THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId, Stream,
};
use libp2p_stream::{Control, IncomingStreams, OpenStreamError};
use nomos_da_messages::{
    packing::{pack_to_writer, unpack_from_reader},
    replication::{self, ReplicationRequest},
};
use subnetworks_assignations::MembershipHandler;
use thiserror::Error;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::error;

use crate::{protocol::REPLICATION_PROTOCOL, SubnetworkId};

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("Stream disconnected: {error}")]
    Io {
        peer_id: PeerId,
        error: std::io::Error,
    },
    #[error("Error dialing peer [{peer_id}]: {error}")]
    OpenStreamError {
        peer_id: PeerId,
        error: OpenStreamError,
    },
}

impl ReplicationError {
    #[must_use]
    pub const fn peer_id(&self) -> Option<&PeerId> {
        match self {
            Self::OpenStreamError { peer_id, .. } | Self::Io { peer_id, .. } => Some(peer_id),
        }
    }
}

impl Clone for ReplicationError {
    fn clone(&self) -> Self {
        match self {
            Self::Io { peer_id, error } => Self::Io {
                peer_id: *peer_id,
                error: std::io::Error::new(error.kind(), error.to_string()),
            },
            Self::OpenStreamError { peer_id, error } => Self::OpenStreamError {
                peer_id: *peer_id,
                error: match error {
                    OpenStreamError::UnsupportedProtocol(protocol) => {
                        OpenStreamError::UnsupportedProtocol(protocol.clone())
                    }
                    OpenStreamError::Io(error) => {
                        OpenStreamError::Io(std::io::Error::new(error.kind(), error.to_string()))
                    }
                    err => OpenStreamError::Io(std::io::Error::other(err.to_string())),
                },
            },
        }
    }
}

#[derive(Debug)]
pub enum ReplicationEvent {
    /// Received a n
    IncomingMessage {
        message: Box<replication::ReplicationRequest>,
    },
    /// Something went wrong receiving the blob
    ReplicationError { error: ReplicationError },
}

impl ReplicationEvent {
    #[must_use]
    pub fn blob_size(&self) -> Option<usize> {
        match self {
            Self::IncomingMessage { message } => Some(message.blob.data.column_len()),
            Self::ReplicationError { .. } => None,
        }
    }
}

type ReadStream = ReadHalf<Stream>;
type WriteStream = WriteHalf<Stream>;

type ReadTask = BoxFuture<
    'static,
    Result<(PeerId, replication::ReplicationRequest, ReadStream), (ReplicationError, ReadStream)>,
>;
type WriteTask = BoxFuture<'static, Result<(PeerId, WriteStream), (ReplicationError, WriteStream)>>;

pub struct ReplicationBehaviour<Membership: MembershipHandler> {
    local_peer_id: PeerId,
    membership: Membership,
    stream_behaviour: libp2p_stream::Behaviour,
    incoming_streams: IncomingStreams,
    connected_peers: HashSet<PeerId>,
    idle_write_streams: HashMap<PeerId, WriteStream>,
    read_tasks: FuturesUnordered<ReadTask>,
    write_tasks: FuturesUnordered<WriteTask>,
    pending_out_streams_sender: UnboundedSender<PeerId>,
    pending_out_streams: BoxStream<'static, Result<(PeerId, Stream), ReplicationError>>,
    pending_blobs_sender: UnboundedSender<ReplicationRequest>,
    pending_blobs_stream: BoxStream<'static, ReplicationRequest>,
    seen_message_cache: IndexSet<(Vec<u8>, SubnetworkId)>,
    to_replicate: HashMap<PeerId, VecDeque<ReplicationRequest>>,
    to_close: VecDeque<WriteStream>,
    /// Waker for dispersal polling
    waker: Option<Waker>,
}

impl<Membership> ReplicationBehaviour<Membership>
where
    Membership: MembershipHandler + 'static,
    Membership::NetworkId: Send,
{
    pub fn new(peer_id: PeerId, membership: Membership) -> Self {
        let stream_behaviour = libp2p_stream::Behaviour::new();
        let mut stream_control = stream_behaviour.new_control();

        let incoming_streams = stream_control
            .accept(REPLICATION_PROTOCOL)
            .expect("Just a single accept to protocol is valid");

        let (pending_out_streams_sender, receiver) = mpsc::unbounded_channel();
        let pending_out_streams = UnboundedReceiverStream::new(receiver)
            .zip(futures::stream::repeat(stream_control))
            .then(|(peer_id, control)| Self::open_stream(peer_id, control))
            .boxed();

        let (pending_blobs_sender, receiver) = mpsc::unbounded_channel();
        let pending_blobs_stream = UnboundedReceiverStream::new(receiver).boxed();

        Self {
            local_peer_id: peer_id,
            membership,
            stream_behaviour,
            incoming_streams,
            connected_peers: HashSet::new(),
            idle_write_streams: HashMap::new(),
            read_tasks: FuturesUnordered::new(),
            write_tasks: FuturesUnordered::new(),
            pending_out_streams_sender,
            pending_out_streams,
            pending_blobs_sender,
            pending_blobs_stream,
            seen_message_cache: IndexSet::new(),
            to_replicate: HashMap::new(),
            to_close: VecDeque::new(),
            waker: None,
        }
    }

    /// Open a new stream from the underlying control to the provided peer
    async fn open_stream(
        peer_id: PeerId,
        mut control: Control,
    ) -> Result<(PeerId, Stream), ReplicationError> {
        let stream = control
            .open_stream(peer_id, REPLICATION_PROTOCOL)
            .await
            .map_err(|error| ReplicationError::OpenStreamError { peer_id, error })?;
        Ok((peer_id, stream))
    }

    pub fn update_membership(&mut self, membership: Membership) {
        self.membership = membership;
    }

    pub fn try_wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    pub fn send_message(&mut self, message: ReplicationRequest) {
        if self.seen_message_cache.contains(&message.id()) {
            return;
        }
        self.seen_message_cache.insert(message.id());
        if let Err(e) = self.pending_blobs_sender.send(message) {
            error!("Error writing to replication sender: {e}");
        }
        self.try_wake();
    }
}

impl<Membership> ReplicationBehaviour<Membership>
where
    Membership: MembershipHandler<Id = PeerId, NetworkId = SubnetworkId> + 'static,
{
    /// Stream handling messages task.
    /// This task handles a single message receive.
    async fn read_message(
        peer_id: PeerId,
        mut stream: ReadStream,
    ) -> Result<(PeerId, replication::ReplicationRequest, ReadStream), (ReplicationError, ReadStream)>
    {
        let message: replication::ReplicationRequest = match unpack_from_reader(&mut stream).await {
            Ok(message) => message,
            Err(error) => {
                return Err((ReplicationError::Io { peer_id, error }, stream));
            }
        };

        Ok((peer_id, message, stream))
    }

    async fn write_message(
        peer_id: PeerId,
        message: replication::ReplicationRequest,
        mut stream: WriteStream,
    ) -> Result<(PeerId, WriteStream), (ReplicationError, WriteStream)> {
        pack_to_writer(&message, &mut stream)
            .await
            .unwrap_or_else(|_| {
                panic!("Message should always be serializable.\nMessage: '{message:?}'",)
            });

        if let Err(error) = stream.flush().await {
            return Err((ReplicationError::Io { peer_id, error }, stream));
        }

        Ok((peer_id, stream))
    }

    fn replicate_blob(
        message: &ReplicationRequest,
        write_tasks: &FuturesUnordered<WriteTask>,
        connected_peers: &HashSet<PeerId>,
        idle_write_streams: &mut HashMap<PeerId, WriteStream>,
        to_replicate: &mut HashMap<PeerId, VecDeque<ReplicationRequest>>,
        local_peer_id: &PeerId,
        membership: &Membership,
    ) {
        let mut members = membership.members_of(&message.subnetwork_id);
        members.remove(local_peer_id);
        let peers = members
            .iter()
            .filter(|peer_id| connected_peers.contains(peer_id));

        for peer in peers {
            if let Some(stream) = idle_write_streams.remove(peer) {
                // push a task if the stream is immediately available
                let fut = Self::write_message(*peer, message.clone(), stream).boxed();
                write_tasks.push(fut);
            } else {
                // otherwise queue the message
                to_replicate
                    .entry(*peer)
                    .or_default()
                    .push_back(message.clone());
            }
        }
    }

    fn handle_connection_closed(&mut self, peer_id: PeerId) {
        if self.connected_peers.remove(&peer_id) {
            _ = self.to_replicate.remove(&peer_id);
            _ = self.idle_write_streams.remove(&peer_id);
        }
    }
}

impl<M: MembershipHandler<Id = PeerId, NetworkId = SubnetworkId> + 'static> NetworkBehaviour
    for ReplicationBehaviour<M>
{
    type ConnectionHandler = Either<
        <libp2p_stream::Behaviour as NetworkBehaviour>::ConnectionHandler,
        libp2p::swarm::dummy::ConnectionHandler,
    >;
    type ToSwarm = ReplicationEvent;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        if !self.membership.is_neighbour(&self.local_peer_id, &peer_id) {
            return Ok(Either::Right(libp2p::swarm::dummy::ConnectionHandler));
        }
        self.connected_peers.insert(peer_id);
        self.stream_behaviour
            .handle_established_inbound_connection(connection_id, peer_id, local_addr, remote_addr)
            .map(Either::Left)
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        if !self.membership.is_neighbour(&self.local_peer_id, &peer_id) {
            return Ok(Either::Right(libp2p::swarm::dummy::ConnectionHandler));
        }
        if let Err(e) = self.pending_out_streams_sender.send(peer_id) {
            error!("Error requesting stream for peer {peer_id}: {e}");
        }
        self.connected_peers.insert(peer_id);
        self.try_wake(); // wake to setup read and write streams.
        self.stream_behaviour
            .handle_established_outbound_connection(
                connection_id,
                peer_id,
                addr,
                role_override,
                port_use,
            )
            .map(Either::Left)
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.stream_behaviour.on_swarm_event(event);
        if let FromSwarm::ConnectionClosed(ConnectionClosed { peer_id, .. }) = event {
            self.handle_connection_closed(peer_id);
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        let Either::Left(event) = event;
        self.stream_behaviour
            .on_connection_handler_event(peer_id, connection_id, event);
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        let Self {
            local_peer_id,
            membership,
            incoming_streams,
            read_tasks,
            write_tasks,
            pending_out_streams,
            pending_blobs_stream,
            seen_message_cache,
            connected_peers,
            idle_write_streams,
            to_replicate,
            to_close,
            ..
        } = self;

        self.waker = Some(cx.waker().clone());

        // Schedule read_tasks for new incoming streams.
        if let Poll::Ready(Some((peer_id, stream))) = incoming_streams.poll_next_unpin(cx) {
            let (read_stream, write_stream) = stream.split();
            idle_write_streams.insert(peer_id, write_stream);
            read_tasks.push(Self::read_message(peer_id, read_stream).boxed());
            cx.waker().wake_by_ref();
        }

        // Schedule read_tasks for new outgoing streams.
        if let Poll::Ready(Some(Ok((peer_id, stream)))) = pending_out_streams.poll_next_unpin(cx) {
            let (read_stream, write_stream) = stream.split();
            idle_write_streams.insert(peer_id, write_stream);
            read_tasks.push(Self::read_message(peer_id, read_stream).boxed());
            cx.waker().wake_by_ref();
        }

        // Shedule replication to the subnetwork peers if new blob arrived from
        // dispersal behaviour.
        if let Poll::Ready(Some(message)) = pending_blobs_stream.poll_next_unpin(cx) {
            Self::replicate_blob(
                &message,
                write_tasks,
                connected_peers,
                idle_write_streams,
                to_replicate,
                local_peer_id,
                membership,
            );
        }

        // Check if any write task finished.
        if let Poll::Ready(Some(future_result)) = write_tasks.poll_next_unpin(cx) {
            cx.waker().wake_by_ref();
            match future_result {
                Ok((peer_id, write_stream)) => {
                    if let Some(message) =
                        to_replicate.get_mut(&peer_id).and_then(VecDeque::pop_front)
                    {
                        let fut = Self::write_message(peer_id, message, write_stream).boxed();
                        write_tasks.push(fut);
                    } else {
                        idle_write_streams.insert(peer_id, write_stream);
                    }
                }
                Err((error, stream)) => {
                    // Error will be propagated by failing read.
                    if let Some(peer_id) = error.peer_id() {
                        _ = self.to_replicate.remove(peer_id);
                        _ = self.idle_write_streams.remove(peer_id);
                    }
                    to_close.push_back(stream);
                }
            }
        }

        // Check if some read task finished.
        if let Poll::Ready(Some(future_result)) = read_tasks.poll_next_unpin(cx) {
            cx.waker().wake_by_ref();
            match future_result {
                Ok((peer_id, message, stream)) => {
                    // Stream is ready to receive new messages.
                    read_tasks.push(Self::read_message(peer_id, stream).boxed());

                    // If this is a unseen message - propagate.
                    if !seen_message_cache.contains(&message.id()) {
                        seen_message_cache.insert(message.id());
                        return Poll::Ready(ToSwarm::GenerateEvent(
                            ReplicationEvent::IncomingMessage {
                                message: Box::new(message),
                            },
                        ));
                    }
                }
                Err((error, _stream)) => {
                    // To close the stream we need to call `close` on AsyncWritter.
                    // ReadStream is just dropped.
                    return Poll::Ready(ToSwarm::GenerateEvent(
                        ReplicationEvent::ReplicationError { error },
                    ));
                }
            }
        }

        // Discard stream, if still pending pushback to close later.
        if let Some(mut stream) = to_close.pop_front() {
            if stream.close().poll_unpin(cx).is_pending() {
                to_close.push_back(stream);
                cx.waker().wake_by_ref();
            }
        }

        Poll::Pending
    }
}
