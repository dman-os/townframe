use super::*;

/// Centralized binary identifier conversions used by the SQLite store.
///
/// SQLite stores these identifiers as their canonical fixed-width byte form;
/// keeping the conversions here prevents individual query implementations from
/// inventing incompatible encodings.
pub(crate) struct IdCodec;

impl IdCodec {
    pub(crate) fn tree_blob(id: SedimentreeId) -> Vec<u8> {
        id.as_bytes().to_vec()
    }
    pub(crate) fn obj_id(id: SedimentreeId) -> ObjId {
        ObjId(Byte32Id::new(*id.as_bytes()))
    }
    pub(crate) fn commit_blob(id: CommitId) -> Vec<u8> {
        id.as_bytes().to_vec()
    }
    pub(crate) fn decode_id(bytes: Vec<u8>) -> Result<[u8; 32], SqliteBigRepoStoreError> {
        bytes
            .try_into()
            .map_err(|_| SqliteBigRepoStoreError::InvalidRecord)
    }
    pub(crate) fn part_blob(id: PartId) -> Vec<u8> {
        SqliteCore::part_blob(id)
    }
    pub(crate) fn obj_blob(id: ObjId) -> Vec<u8> {
        SqliteCore::obj_blob(id)
    }
    pub(crate) fn peer_blob(id: PeerId) -> Vec<u8> {
        SqliteCore::peer_blob(id)
    }
    pub(crate) fn buck_i64(id: BuckId) -> i64 {
        SqliteCore::buck_i64(id)
    }
    pub(crate) fn buck_id(value: i64) -> BuckId {
        SqliteCore::buck_id(value)
    }
    pub(crate) fn u64_from_db(value: i64) -> u64 {
        SqliteCore::u64_from_db(value)
    }
    pub(crate) fn part_from_blob(blob: Vec<u8>) -> PartId {
        SqliteCore::part_from_blob(blob)
    }
    pub(crate) fn obj_from_blob(blob: Vec<u8>) -> ObjId {
        SqliteCore::obj_from_blob(blob)
    }
}

impl SqliteBigRepoStore {
    pub(crate) fn part_blob(id: PartId) -> Vec<u8> {
        IdCodec::part_blob(id)
    }
    pub(crate) fn obj_blob(id: ObjId) -> Vec<u8> {
        IdCodec::obj_blob(id)
    }
    pub(crate) fn peer_blob(id: PeerId) -> Vec<u8> {
        IdCodec::peer_blob(id)
    }
    pub(crate) fn buck_i64(id: BuckId) -> i64 {
        IdCodec::buck_i64(id)
    }
    pub(crate) fn buck_id(value: i64) -> BuckId {
        IdCodec::buck_id(value)
    }
    pub(crate) fn u64_from_db(value: i64) -> u64 {
        IdCodec::u64_from_db(value)
    }
    pub(crate) fn part_from_blob(blob: Vec<u8>) -> PartId {
        IdCodec::part_from_blob(blob)
    }
    pub(crate) fn obj_from_blob(blob: Vec<u8>) -> ObjId {
        IdCodec::obj_from_blob(blob)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_width_identifier_codecs_round_trip() {
        let bytes = [0xA5; 32];
        let tree = SedimentreeId::new(bytes);
        assert_eq!(IdCodec::decode_id(IdCodec::tree_blob(tree)).unwrap(), bytes);
        let commit = CommitId::new(bytes);
        assert_eq!(
            IdCodec::decode_id(IdCodec::commit_blob(commit)).unwrap(),
            bytes
        );
        let part = PartId::new(bytes);
        assert_eq!(IdCodec::part_from_blob(IdCodec::part_blob(part)), part);
        let obj = ObjId::new(bytes);
        assert_eq!(IdCodec::obj_from_blob(IdCodec::obj_blob(obj)), obj);
        let peer = PeerId::new(bytes);
        assert_eq!(IdCodec::peer_blob(peer), peer.as_bytes().to_vec());
        let buck = BuckId::new(3, 17);
        assert_eq!(IdCodec::buck_id(IdCodec::buck_i64(buck)), buck);
        assert_eq!(IdCodec::u64_from_db(42), 42);
    }

    #[test]
    fn identifier_codec_rejects_non_fixed_width_values() {
        assert!(IdCodec::decode_id(vec![0; 31]).is_err());
        assert!(IdCodec::decode_id(vec![0; 33]).is_err());
    }
}
