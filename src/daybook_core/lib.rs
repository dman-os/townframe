mod interlude {
    pub use std::{
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock},
    };
    pub use utils_rs::prelude::*;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use interlude::*;

struct Ledger {
    aliases: DHashMap<CHeapStr, CHeapStr>,
    txns: Vec<Txn>,
}

struct Posting {
    account: CHeapStr,
    currency: CHeapStr,
    amount: rust_decimal::Decimal,
}

struct Txn {
    ts: time::OffsetDateTime,
    desc: Option<String>,
    postings: Vec<Posting>,
}

// #[test]
fn test() -> Res<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let ledger = Ledger {
                aliases: [
                    ("a".into(), "assets".into()),
                    ("cash".into(), "assets:cash".into()),
                    ("b".into(), "assets:banking".into()),
                    ("c".into(), "liabilities:credit".into()),
                    ("l".into(), "liabilities".into()),
                    ("e".into(), "expenses".into()),
                    ("taxi".into(), "expenses:transport:taxi".into()),
                    ("food".into(), "expenses:personal:food".into()),
                    ("p".into(), "expenses:people".into()),
                    ("i".into(), "income".into()),
                ]
                .into_iter()
                .collect(),
                txns: vec![Txn {
                    ts: time::OffsetDateTime::now_utc(),
                    desc: Some("First".into()),
                    postings: [
                        Posting {
                            account: "food".into(),
                            currency: "$".into(),
                            amount: 12345.into(),
                        },
                        Posting {
                            account: "cash".into(),
                            currency: "$".into(),
                            amount: (-12345).into(),
                        },
                    ]
                    .into_iter()
                    .collect(),
                }],
            };
            //
            eyre::Ok(())
        })
}
