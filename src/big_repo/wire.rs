use sedimentree_core::{
    codec::{
        decode::Decode,
        encode::Encode,
        error::{DecodeError, InvalidSchema},
    },
    id::SedimentreeId,
};
use subduction_core::connection::message::{
    BatchSyncResponse, SyncMessage, TryAsBatchSyncResponse, TryAsSubscribeRequest, MESSAGE_SCHEMA,
};
use subduction_keyhive::{KeyhiveMessage, KEYHIVE_SCHEMA};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BigRepoWireMessage {
    Sync(Box<SyncMessage>),
    Keyhive(KeyhiveMessage),
}

impl From<SyncMessage> for BigRepoWireMessage {
    fn from(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }
}

impl From<KeyhiveMessage> for BigRepoWireMessage {
    fn from(msg: KeyhiveMessage) -> Self {
        Self::Keyhive(msg)
    }
}

impl TryAsBatchSyncResponse for BigRepoWireMessage {
    fn try_as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            BigRepoWireMessage::Sync(sync) => sync.try_as_batch_sync_response(),
            BigRepoWireMessage::Keyhive(_) => None,
        }
    }
}

impl TryAsSubscribeRequest for BigRepoWireMessage {
    fn try_as_subscribe_request(&self) -> Option<SedimentreeId> {
        match self {
            BigRepoWireMessage::Sync(sync) => sync.try_as_subscribe_request(),
            BigRepoWireMessage::Keyhive(_) => None,
        }
    }
}

impl Encode for BigRepoWireMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Sync(msg) => Encode::encode(msg.as_ref()),
            Self::Keyhive(msg) => msg.encode(),
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::Sync(msg) => msg.encoded_size(),
            Self::Keyhive(msg) => msg.encoded_size(),
        }
    }
}

impl Decode for BigRepoWireMessage {
    const MIN_SIZE: usize = 8; // schema(4) + total_size(4)

    fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < 4 {
            return Err(DecodeError::MessageTooShort {
                type_name: "BigRepoWireMessage schema",
                need: 4,
                have: buf.len(),
            });
        }

        let schema: [u8; 4] =
            buf.get(0..4)
                .and_then(|s| s.try_into().ok())
                .ok_or(DecodeError::MessageTooShort {
                    type_name: "BigRepoWireMessage schema",
                    need: 4,
                    have: buf.len(),
                })?;

        match schema {
            MESSAGE_SCHEMA => {
                SyncMessage::try_decode(buf).map(|m| BigRepoWireMessage::Sync(Box::new(m)))
            }
            KEYHIVE_SCHEMA => KeyhiveMessage::try_decode(buf).map(BigRepoWireMessage::Keyhive),
            _ => Err(InvalidSchema {
                expected: MESSAGE_SCHEMA,
                got: schema,
            }
            .into()),
        }
    }
}
