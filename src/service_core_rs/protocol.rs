use crate::{Subscription, Unary, XrpcOperation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolOperations {
    pub unary: &'static [XrpcOperation],
    pub stream: &'static [XrpcOperation],
}

pub trait Protocol: irpc::Service {
    const OPERATIONS: ProtocolOperations;
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
