use crate::Envelope;
use crate::Reply;
use crate::Subscription;
use crate::Unary;
use crate::XrpcOperation;
use irpc::Channels;
use irpc::WithChannels;
use irpc::channel::none::NoReceiver;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolOperations {
    pub unary: &'static [XrpcOperation],
    pub stream: &'static [XrpcOperation],
}

pub trait Protocol: irpc::Service {
    const OPERATIONS: ProtocolOperations;

    /// Calls `visitor` once per unary operation of this protocol, with the operation's type. A
    /// `Client<Self>` can only call an operation whose type it names, so this is how code that
    /// is generic over protocols (a by-NSID router) obtains one typed route per operation
    /// without a hand-written list that could drift from the protocol declaration.
    fn visit_unary<V: UnaryVisitor<Self>>(visitor: &mut V)
    where
        Self: Sized;
}

/// A callback over the unary operations of `Proto`; see [`Protocol::visit_unary`]. The bounds
/// are exactly what `Client::call` needs, so an implementation can hold a client and call it.
pub trait UnaryVisitor<Proto: irpc::Service> {
    fn visit<P>(&mut self)
    where
        P: Unary,
        Proto: From<Envelope<P>>,
        Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
        Envelope<P>: Channels<Proto, Tx = Reply<P>, Rx = NoReceiver>;
}

pub trait UnaryOperation: Unary {
    const DECLARATION: XrpcOperation = Self::XRPC;
}
impl<P: Unary> UnaryOperation for P {}

pub trait StreamOperation: Subscription {
    const DECLARATION: XrpcOperation = Self::XRPC;
}
impl<P: Subscription> StreamOperation for P {}

#[macro_export]
macro_rules! declare_service_protocol {
    ($vis:vis enum $protocol:ident => $message:ident {
        unary { $( $variant:ident($params:ty) ),* $(,)? }
        stream { $( $stream_variant:ident($stream_params:ty) ),* $(,)? }
    }) => {
        #[$crate::__irpc::rpc_requests(message = $message)]
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        $vis enum $protocol {
            $( #[rpc(tx = $crate::Reply<$params>)] $variant($crate::Envelope<$params>), )*
            $( #[rpc(tx = $crate::EventReply<$stream_params>)] $stream_variant($crate::Envelope<$stream_params>), )*
        }
        impl $crate::Protocol for $protocol {
            const OPERATIONS: $crate::ProtocolOperations = $crate::ProtocolOperations {
                unary: &[$(<$params as $crate::protocol::UnaryOperation>::DECLARATION),*],
                stream: &[$(<$stream_params as $crate::protocol::StreamOperation>::DECLARATION),*],
            };
            fn visit_unary<V: $crate::protocol::UnaryVisitor<Self>>(visitor: &mut V) {
                $( visitor.visit::<$params>(); )*
            }
        }
        impl $protocol {
            pub const OPERATIONS: $crate::ProtocolOperations = <Self as $crate::Protocol>::OPERATIONS;
        }
    };
}

#[macro_export]
macro_rules! service_operation {
    ($params:ty => $output:ty, $error:ty, $kind:ident($nsid:literal)) => {
        impl $crate::Operation for $params {
            type Output = $output;
            type Error = $error;
            const XRPC: $crate::XrpcOperation = $crate::XrpcOperation::$kind($nsid)
                .with_schemas(stringify!($output), stringify!($error));
        }
    };
    ($params:ty => stream $event:ty, $error:ty, $nsid:literal) => {
        impl $crate::Operation for $params {
            type Output = $crate::EventStream<$event, $error>;
            type Error = $error;
            const XRPC: $crate::XrpcOperation = $crate::XrpcOperation::subscription($nsid)
                .with_schemas(stringify!($event), stringify!($error));
        }
        impl $crate::Subscription for $params {
            type Event = $event;
        }
    };
}
