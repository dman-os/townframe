use crate::interlude::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerPartsSummaryRequest {
    /// The parts that that are desired to be replayed from the peer
    /// and the cursor on which replay stopped last
    pub parts: Map<PartKey, CursorIndex>,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct PeerPartsSummaryResponse {
        pub parts: Map<
            PartKey,
            pub enum PeerPartSummary {
                /// The different replay strats avail for the part
                ReplayAvail(
                    Vec<pub enum PeerPartStratSummary {
                        CursorPartSummary(pub struct {
                            pub latest_cursor: CursorIndex,
                            /// New events visible to the part since the provided cursor.
                            /// A simple latest_cursor - cur_cursor won't do
                            /// since the cursor is global across parts
                            pub dirty_count: u64,
                        }),
                        BucketPartSummary (pub struct {
                            pub deepest_bucket_level: BuckLevel,
                            pub member_count: u64,
                            /// New changes visible to the part since the provided cursor.
                            pub dirty_count: u64,
                        }),
                    }>,
                )
                // The bucket is either unknown on the node or the asking node is not
                // authorized for it
                Unknown,
            }
        >
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum PeerPartsSummaryError {
    /// UnkownParts {unkown_parts:?}
    UnkownParts { unkown_parts: Vec<PartKey> },
}
