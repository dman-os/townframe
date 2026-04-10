pub mod node;
pub mod peer;
pub mod protocol;
pub mod store;

pub trait PartitionAccessPolicy: Send + Sync + 'static {
    fn can_access_partition(
        &self,
        peer: &protocol::PeerKey,
        partition_id: &protocol::PartitionId,
    ) -> bool;
}

pub struct AllowAllPartitionAccessPolicy;

impl PartitionAccessPolicy for AllowAllPartitionAccessPolicy {
    fn can_access_partition(
        &self,
        _peer: &protocol::PeerKey,
        _partition_id: &protocol::PartitionId,
    ) -> bool {
        true
    }
}
