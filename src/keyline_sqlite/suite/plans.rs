//! Every statement the backend runs is served by an index, except the three that read the whole
//! set because their answer is the whole set.
//!
//! The backend runs through a database wrapper that records each distinct statement text with
//! one set of its parameters, over every query and an insert; then `EXPLAIN QUERY PLAN` is
//! asked of each. A plan line that scans `keyline_certificates` fails the test unless the
//! statement is one of the whole-set reads, which are pinned to the scan they should do. The scan
//! of an insert's own `VALUES` list (and the constant row an `EXISTS` is selected from) is as
//! big as the batch, not the set.

use std::collections::BTreeMap;
use std::sync::Arc;

use future_form::Local;
use futures::executor::block_on;
use keyline::contract::Keyline;
use keyline::power::Power;

use keyline::test_utils::conformance::ALICE;
use keyline::test_utils::conformance::BOB;
use keyline::test_utils::conformance::DOC;
use keyline::test_utils::conformance::OWNERS;
use keyline::test_utils::conformance::d;
use keyline::test_utils::conformance::r;
use keyline::test_utils::id;
use sqlite_rs::Database;
use sqlite_rs::SqlValue;

use super::fresh_db;
use super::recording::Recording;
use super::sign;
use crate::SqliteKeyline;
use crate::sqlite::queries;

/// The plan of `sql`, one line per step.
async fn plan(db: &Database, sql: &str, params: &[SqlValue]) -> Vec<String> {
    db.execute(&format!("EXPLAIN QUERY PLAN {sql}"), params.to_vec())
        .await
        .unwrap()
        .result_set
        .rows
        .iter()
        .map(|row| row.get::<String>("detail").unwrap())
        .collect()
}

#[test]
fn every_statement_is_served_by_an_index() {
    block_on(async {
        let recording = Recording::new(fresh_db());
        let log = Arc::clone(&recording.log);
        let store = SqliteKeyline::<(), _>::new(recording.clone());
        let bob = d(ALICE, BOB, DOC, Power::Edit);
        let batch = vec![
            sign(d(DOC, OWNERS, DOC, Power::Admin)),
            sign(d(DOC, ALICE, OWNERS, Power::Admin)),
            sign(bob),
            sign(r::<()>(ALICE, &bob)),
        ];
        let digests = [d(DOC, OWNERS, DOC, Power::Admin).digest()];
        let cert = keyline::certificate::CertificateId::from(digests[0]);
        let version = Keyline::<Local>::insert(&store, batch).await.unwrap();
        assert!(version > keyline::contract::Version::INITIAL);
        let k = &store;
        Keyline::<Local>::contains(k, cert).await.unwrap();
        Keyline::<Local>::get(k, cert).await.unwrap();
        Keyline::<Local>::revocations_of(k, bob.digest())
            .await
            .unwrap();
        Keyline::<Local>::delegations_to(k, id(DOC), id(ALICE))
            .await
            .unwrap();
        Keyline::<Local>::subjects(k).await.unwrap();
        Keyline::<Local>::digest(k).await.unwrap();
        Keyline::<Local>::certificates(k).await.unwrap();
        // The evaluated queries, each through a store that has not seen the inserts; the document is
        // disputed by the revocation, so the first one computes its live set; the delegation's
        // liveness, rooted at its own subject; and a subset.
        let cold = SqliteKeyline::<(), _>::new(recording);
        Keyline::<Local>::members(&cold, id(DOC)).await.unwrap();
        Keyline::<Local>::effective_power(&cold, id(DOC), id(BOB))
            .await
            .unwrap();
        Keyline::<Local>::positive_power(&cold, id(DOC), id(BOB))
            .await
            .unwrap();
        Keyline::<Local>::closure(&cold, id(DOC)).await.unwrap();
        Keyline::<Local>::is_live(&cold, bob.digest()).await.unwrap();
        Keyline::<Local>::conveyed_power(&cold, bob.digest())
            .await
            .unwrap();
        Keyline::<Local>::members_for_subset(&cold, [cert].into(), id(DOC))
            .await
            .unwrap();

        let statements = log.lock().unwrap().clone();
        assert!(
            statements.len() >= 40,
            "the workload ran {} distinct statements",
            statements.len()
        );
        let db = fresh_db();
        let mut scans = Vec::new();
        let mut plans = BTreeMap::new();
        for (sql, params) in &statements {
            let lines = plan(&db, sql, params).await;
            let whole_set = [
                queries::ALL_CERTIFICATES_SQL,
                queries::ALL_CERTIFICATE_IDS_SQL,
                queries::SUBJECTS_SQL,
            ]
            .contains(&sql.as_str());
            for line in &lines {
                // A search bounded only by `kind` walks every certificate of that kind: a scan that
                // looks like an index search.
                let kind_range = line.ends_with("PRIMARY KEY (kind=?)")
                    && !line.starts_with("SEARCH nw ")
                    && !line.starts_with("SEARCH keyline_new ");
                // The tables a scan is allowed to drive: the four levels, and the batch, the roots
                // and the contexts of a computation in progress, each bounded by the work at hand.
                let bounded = ["lv", "nw", "rt", "x", "keyline_new", "keyline_root", "keyline_ctx_use", "keyline_level"]
                    .iter()
                    .any(|table| {
                        line.strip_prefix("SCAN ")
                            .is_some_and(|rest| rest.split_whitespace().next() == Some(table))
                    });
                let scan = line.starts_with("SCAN")
                    && !bounded
                    && !line.starts_with("SCAN (subquery")
                    && !line.contains("VALUES CLAUSE")
                    && !line.contains("CONSTANT ROW");
                if (scan && !whole_set) || (kind_range && !whole_set) {
                    scans.push(format!(
                        "{line}\n    in {}",
                        sql.split_whitespace().collect::<Vec<_>>().join(" ")
                    ));
                }
            }
            plans.insert(sql.as_str(), lines);
        }
        assert!(
            scans.is_empty(),
            "statements that scan:\n{}",
            scans.join("\n")
        );

        let plan_of = |sql: &str| {
            plans
                .get(sql)
                .unwrap_or_else(|| panic!("never ran: {sql}"))
                .join("\n")
        };
        // The access paths the schema's indexes are for.
        let by_key = plan_of(queries::CERTIFICATE_EXISTS_SQL);
        assert!(
            by_key.contains("PRIMARY KEY (kind=? AND digest=?)"),
            "{by_key}"
        );
        let by_key = plan_of(queries::CERTIFICATE_SIGNED_SQL);
        assert!(
            by_key.contains("PRIMARY KEY (kind=? AND digest=?)"),
            "{by_key}"
        );
        let edges = plan_of(queries::DELEGATIONS_TO_SQL);
        assert!(
            edges.contains("keyline_delegations_by_subject_audience (subject=? AND audience=?"),
            "{edges}"
        );
        let revocations = plan_of(queries::REVOCATIONS_OF_SQL);
        assert!(
            revocations.contains("keyline_revocations_by_target (revoke=?"),
            "{revocations}"
        );
        // The access paths the evaluation's statements are for: a round reads only its delta, by
        // epoch; a delegation's row is a primary-key lookup; a subset is by primary key.
        for sql in [queries::REACH_ROUND_SQL, queries::LIVE_ROUND_SQL] {
            let rounds = plan_of(sql);
            assert!(
                rounds.contains("_by_epoch (epoch=?)"),
                "a round must start from its delta:\n{rounds}"
            );
        }
        let subject = plan_of(queries::DELEGATION_ROW_SQL);
        assert!(
            subject.contains("PRIMARY KEY (kind=? AND digest=?)"),
            "{subject}"
        );
        let plan_prefix = |prefix: &str| {
            plans
                .iter()
                .find(|(sql, _)| sql.starts_with(prefix))
                .map(|(_, lines)| lines.join("\n"))
                .unwrap_or_else(|| panic!("never ran: {prefix}"))
        };
        let ids = plan_prefix(
            "SELECT kind, digest, signed FROM keyline_certificates WHERE (kind, digest) IN",
        );
        assert!(ids.contains("PRIMARY KEY (kind=? AND digest=?)"), "{ids}");
        for (name, sql) in [
            ("is_disputed", queries::IS_DISPUTED_SQL),
            ("reach_standing", queries::REACH_STANDING_SQL),
            ("reach_members", queries::REACH_MEMBERS_SQL),
            ("live_members", queries::LIVE_MEMBERS_SQL),
            ("live_standing", queries::LIVE_STANDING_SQL),
        ] {
            let lines = plan_of(sql);
            assert!(lines.contains("PRIMARY KEY"), "{name} must be a key search:\n{lines}");
        }
        // The whole-set reads scan, and in the order they return: no sort.
        for sql in [
            queries::ALL_CERTIFICATES_SQL,
            queries::ALL_CERTIFICATE_IDS_SQL,
        ] {
            let scan = plan_of(sql);
            assert!(!scan.contains("TEMP B-TREE"), "{scan}");
        }
        let subjects = plan_of(queries::SUBJECTS_SQL);
        assert!(
            subjects.contains("COVERING INDEX keyline_delegations_by_subject_audience"),
            "{subjects}"
        );
        for (sql, lines) in &plans {
            println!(
                "PLAN {}\n  {}",
                sql.split_whitespace().collect::<Vec<_>>().join(" "),
                lines.join("\n  ")
            );
        }
    });
}
