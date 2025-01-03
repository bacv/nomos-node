use std::{
    collections::VecDeque,
    io,
    task::{Context, Poll, Waker},
};

use futures::{future::BoxFuture, AsyncReadExt, AsyncWriteExt, FutureExt};
use libp2p::{
    core::upgrade::ReadyUpgrade,
    swarm::{
        handler::{ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound},
        ConnectionHandler, ConnectionHandlerEvent, StreamUpgradeError, SubstreamProtocol,
    },
    Stream, StreamProtocol,
};

// Metrics
const VALUE_FULLY_NEGOTIATED_INBOUND: &str = "fully_negotiated_inbound";
const VALUE_FULLY_NEGOTIATED_OUTBOUND: &str = "fully_negotiated_outbound";
const VALUE_DIAL_UPGRADE_ERROR: &str = "dial_upgrade_error";
const VALUE_IGNORED: &str = "ignored";

const PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/nomos/blend/0.1.0");

// TODO: Consider replacing this struct with libp2p_stream ConnectionHandler
//       because we don't implement persistent emission in the per-connection level anymore.
/// A [`ConnectionHandler`] that handles the blend protocol.
pub struct BlendConnectionHandler {
    inbound_substream: Option<MsgRecvFuture>,
    outbound_substream: Option<OutboundSubstreamState>,
    outbound_msgs: VecDeque<Vec<u8>>,
    pending_events_to_behaviour: VecDeque<ToBehaviour>,
    waker: Option<Waker>,
}

type MsgSendFuture = BoxFuture<'static, Result<Stream, io::Error>>;
type MsgRecvFuture = BoxFuture<'static, Result<(Stream, Vec<u8>), io::Error>>;

enum OutboundSubstreamState {
    /// A request to open a new outbound substream is being processed.
    PendingOpenSubstream,
    /// An outbound substream is open and ready to send messages.
    Idle(Stream),
    /// A message is being sent on the outbound substream.
    PendingSend(MsgSendFuture),
}

impl BlendConnectionHandler {
    pub fn new() -> Self {
        Self {
            inbound_substream: None,
            outbound_substream: None,
            outbound_msgs: VecDeque::new(),
            pending_events_to_behaviour: VecDeque::new(),
            waker: None,
        }
    }

    fn try_wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl Default for BlendConnectionHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum FromBehaviour {
    /// A message to be sent to the connection.
    Message(Vec<u8>),
}

#[derive(Debug)]
pub enum ToBehaviour {
    /// An outbound substream has been successfully upgraded for the blend protocol.
    FullyNegotiatedOutbound,
    /// An outbound substream was failed to be upgraded for the blend protocol.
    NegotiationFailed,
    /// A message has been received from the connection.
    Message(Vec<u8>),
    /// An IO error from the connection
    IOError(io::Error),
}

impl ConnectionHandler for BlendConnectionHandler {
    type FromBehaviour = FromBehaviour;
    type ToBehaviour = ToBehaviour;
    type InboundProtocol = ReadyUpgrade<StreamProtocol>;
    type InboundOpenInfo = ();
    type OutboundProtocol = ReadyUpgrade<StreamProtocol>;
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(ReadyUpgrade::new(PROTOCOL_NAME), ())
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        tracing::info!(gauge.pending_outbound_messages = self.outbound_msgs.len() as u64,);
        tracing::info!(
            gauge.pending_events_to_behaviour = self.pending_events_to_behaviour.len() as u64,
        );

        // Process pending events to be sent to the behaviour
        if let Some(event) = self.pending_events_to_behaviour.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        // Process inbound stream
        // TODO: Measure message frequencies and compare them to the desired frequencies
        //       for connection maintenance defined in the Tier 1 spec.
        tracing::debug!("Processing inbound stream");
        if let Some(msg_recv_fut) = self.inbound_substream.as_mut() {
            match msg_recv_fut.poll_unpin(cx) {
                Poll::Ready(Ok((stream, msg))) => {
                    tracing::debug!("Received message from inbound stream. Notifying behaviour...");
                    self.inbound_substream = Some(recv_msg(stream).boxed());
                    return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                        ToBehaviour::Message(msg),
                    ));
                }
                Poll::Ready(Err(e)) => {
                    tracing::error!("Failed to receive message from inbound stream: {:?}", e);
                    self.inbound_substream = None;
                }
                Poll::Pending => {}
            }
        }

        // Process outbound stream
        tracing::debug!("Processing outbound stream");
        loop {
            match self.outbound_substream.take() {
                // If the request to open a new outbound substream is still being processed, wait more.
                Some(OutboundSubstreamState::PendingOpenSubstream) => {
                    self.outbound_substream = Some(OutboundSubstreamState::PendingOpenSubstream);
                    self.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                // If the substream is idle, and if it's time to send a message, send it.
                Some(OutboundSubstreamState::Idle(stream)) => {
                    match self.outbound_msgs.pop_front() {
                        Some(msg) => {
                            tracing::debug!("Sending message to outbound stream: {:?}", msg);
                            self.outbound_substream = Some(OutboundSubstreamState::PendingSend(
                                send_msg(stream, msg).boxed(),
                            ));
                        }
                        None => {
                            tracing::debug!("Nothing to send to outbound stream");
                            self.outbound_substream = Some(OutboundSubstreamState::Idle(stream));
                            self.waker = Some(cx.waker().clone());
                            return Poll::Pending;
                        }
                    }
                }
                // If a message is being sent, check if it's done.
                Some(OutboundSubstreamState::PendingSend(mut msg_send_fut)) => {
                    match msg_send_fut.poll_unpin(cx) {
                        Poll::Ready(Ok(stream)) => {
                            tracing::debug!("Message sent to outbound stream");
                            self.outbound_substream = Some(OutboundSubstreamState::Idle(stream));
                        }
                        Poll::Ready(Err(e)) => {
                            tracing::error!("Failed to send message to outbound stream: {:?}", e);
                            self.outbound_substream = None;
                            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                                ToBehaviour::IOError(e),
                            ));
                        }
                        Poll::Pending => {
                            self.outbound_substream =
                                Some(OutboundSubstreamState::PendingSend(msg_send_fut));
                            self.waker = Some(cx.waker().clone());
                            return Poll::Pending;
                        }
                    }
                }
                // If there is no outbound substream, request to open a new one.
                None => {
                    self.outbound_substream = Some(OutboundSubstreamState::PendingOpenSubstream);
                    return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(ReadyUpgrade::new(PROTOCOL_NAME), ()),
                    });
                }
            }
        }
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            FromBehaviour::Message(msg) => {
                self.outbound_msgs.push_back(msg);
            }
        }
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        let event_name = match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: stream,
                ..
            }) => {
                tracing::debug!("FullyNegotiatedInbound: Creating inbound substream");
                self.inbound_substream = Some(recv_msg(stream).boxed());
                VALUE_FULLY_NEGOTIATED_INBOUND
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                ..
            }) => {
                tracing::debug!("FullyNegotiatedOutbound: Creating outbound substream");
                self.outbound_substream = Some(OutboundSubstreamState::Idle(stream));
                self.pending_events_to_behaviour
                    .push_back(ToBehaviour::FullyNegotiatedOutbound);
                VALUE_FULLY_NEGOTIATED_OUTBOUND
            }
            ConnectionEvent::DialUpgradeError(e) => {
                tracing::error!("DialUpgradeError: {:?}", e);
                match e.error {
                    StreamUpgradeError::NegotiationFailed => {
                        self.pending_events_to_behaviour
                            .push_back(ToBehaviour::NegotiationFailed);
                    }
                    StreamUpgradeError::Io(e) => {
                        self.pending_events_to_behaviour
                            .push_back(ToBehaviour::IOError(e));
                    }
                    StreamUpgradeError::Timeout => {
                        self.pending_events_to_behaviour
                            .push_back(ToBehaviour::IOError(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "blend protocol negotiation timed out",
                            )));
                    }
                    StreamUpgradeError::Apply(_) => unreachable!(),
                };
                VALUE_DIAL_UPGRADE_ERROR
            }
            event => {
                tracing::debug!("Ignoring connection event: {:?}", event);
                VALUE_IGNORED
            }
        };

        tracing::info!(counter.connection_event = 1, event = event_name);
        self.try_wake();
    }
}

/// Write a message to the stream
async fn send_msg(mut stream: Stream, msg: Vec<u8>) -> io::Result<Stream> {
    let msg_len: u16 = msg.len().try_into().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Message length is too big. Got {}, expected {}",
                msg.len(),
                std::mem::size_of::<u16>()
            ),
        )
    })?;
    stream.write_all(msg_len.to_be_bytes().as_ref()).await?;
    stream.write_all(&msg).await?;
    stream.flush().await?;
    Ok(stream)
}
/// Read a message from the stream
async fn recv_msg(mut stream: Stream) -> io::Result<(Stream, Vec<u8>)> {
    let mut msg_len = [0; std::mem::size_of::<u16>()];
    stream.read_exact(&mut msg_len).await?;
    let msg_len = u16::from_be_bytes(msg_len) as usize;

    let mut buf = vec![0; msg_len];
    stream.read_exact(&mut buf).await?;
    Ok((stream, buf))
}
