use crate::Envelope;
use crate::Reply;
use irpc::Channels;
use irpc::WithChannels;
use irpc::channel::none::NoReceiver;
use method_core::Subscription;
use method_core::Unary;
use method_core::XrpcMethod;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolMethods {
    pub unary: &'static [XrpcMethod],
    pub stream: &'static [XrpcMethod],
}

pub trait Protocol: irpc::Service {
    const METHODS: ProtocolMethods;

    /// Calls `visitor` once per unary method of this protocol, with the method's type. A
    /// `Client<Self>` can only call a method whose type it names, so this is how code that
    /// is generic over protocols (a by-NSID router) obtains one typed route per method
    /// without a hand-written list that could drift from the protocol declaration.
    fn visit_unary<V: UnaryVisitor<Self>>(visitor: &mut V)
    where
        Self: Sized;
}

/// A callback over the unary methods of `Proto`; see [`Protocol::visit_unary`]. The bounds
/// are exactly what `Client::call` needs, so an implementation can hold a client and call it.
pub trait UnaryVisitor<Proto: irpc::Service> {
    fn visit<P>(&mut self)
    where
        P: Unary,
        Proto: From<Envelope<P>>,
        Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
        Envelope<P>: Channels<Proto, Tx = Reply<P>, Rx = NoReceiver>;
}

pub trait UnaryMethod: Unary {
    const DECLARATION: XrpcMethod = Self::XRPC;
}
impl<P: Unary> UnaryMethod for P {}

pub trait StreamMethod: Subscription {
    const DECLARATION: XrpcMethod = Self::XRPC;
}
impl<P: Subscription> StreamMethod for P {}

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
            const METHODS: $crate::ProtocolMethods = $crate::ProtocolMethods {
                unary: &[$(<$params as $crate::protocol::UnaryMethod>::DECLARATION),*],
                stream: &[$(<$stream_params as $crate::protocol::StreamMethod>::DECLARATION),*],
            };
            fn visit_unary<V: $crate::protocol::UnaryVisitor<Self>>(visitor: &mut V) {
                $( visitor.visit::<$params>(); )*
            }
        }
        impl $protocol {
            pub const METHODS: $crate::ProtocolMethods = <Self as $crate::Protocol>::METHODS;
        }
    };
}

#[macro_export]
macro_rules! service_method {
    ($params:ty => $output:ty, $error:ty, $kind:ident($nsid:literal)) => {
        impl ::method_core::Method for $params {
            type Output = $output;
            type Error = $error;
            const XRPC: ::method_core::XrpcMethod = ::method_core::XrpcMethod::$kind($nsid)
                .with_schemas(stringify!($output), stringify!($error));
        }
        ::method_core::__method_schema!($params, $output, $error);
    };
    ($params:ty => stream $event:ty, $error:ty, $nsid:literal) => {
        impl ::method_core::Method for $params {
            type Output = ::method_core::EventStream<$event, $error>;
            type Error = $error;
            const XRPC: ::method_core::XrpcMethod = ::method_core::XrpcMethod::subscription($nsid)
                .with_schemas(stringify!($event), stringify!($error));
        }
        impl ::method_core::Subscription for $params {
            type Event = $event;
        }
        ::method_core::__method_schema!($params, $event, $error);
    };
}
