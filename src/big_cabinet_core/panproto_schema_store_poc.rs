//! POC: PlugsRepo writes Panproto revisions; DocumentsRepo resolves them read-only.

use std::path::Path;

use panproto_mig::Migration;
use panproto_protocols::data_schema::json_schema::parse_json_schema_bundle;
use panproto_schema::Schema;
use panproto_vcs::{
    FsStore, Object, ObjectId, Repository, Store, VcsError, hash::hash_schema,
    tree::resolve_commit_schema,
};

/// The read-only surface DocumentsRepo needs. PlugsRepo owns `Repository` and
/// publishes commits; document reads are pinned to an exact commit ID.
struct SchemaStore {
    store: FsStore,
}

struct StoredMigration {
    source: ObjectId,
    target: ObjectId,
    mapping: Migration,
}

impl SchemaStore {
    fn open(path: &Path) -> Result<Self, VcsError> {
        Ok(Self {
            store: FsStore::open(path)?,
        })
    }

    fn schema_at(&self, revision: &ObjectId) -> Result<Schema, VcsError> {
        let commit = self.commit_at(revision)?;
        resolve_commit_schema(&self.store, &commit)
    }

    fn migration_at(&self, revision: &ObjectId) -> Result<Option<StoredMigration>, VcsError> {
        let commit = self.commit_at(revision)?;
        let Some(migration_id) = commit.migration_id else {
            return Ok(None);
        };

        match self.store.get(&migration_id)? {
            Object::Migration { src, tgt, mapping } => Ok(Some(StoredMigration {
                source: src,
                target: tgt,
                mapping,
            })),
            other => Err(VcsError::WrongObjectType {
                expected: "migration",
                found: other.type_name(),
            }),
        }
    }

    fn commit_at(&self, revision: &ObjectId) -> Result<panproto_vcs::CommitObject, VcsError> {
        match self.store.get(revision)? {
            Object::Commit(commit) => Ok(commit),
            other => Err(VcsError::WrongObjectType {
                expected: "commit",
                found: other.type_name(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    const COMMON_SCHEMA: &str = r#"
        {
            "$id": "https://schemas.example/common.json",
            "$defs": {
                "Address": {
                    "type": "object",
                    "properties": { "city": { "type": "string" } },
                    "required": ["city"]
                }
            }
        }
    "#;

    fn profile_schema(include_display_name: bool) -> Value {
        let mut properties = json!({
            "name": { "type": "string" },
            "address": {
                "$ref": "https://schemas.example/common.json#/$defs/Address"
            }
        });
        if include_display_name {
            properties["displayName"] = json!({ "type": "string" });
        }

        json!({
            "$id": "https://schemas.example/profile.json",
            "type": "object",
            "properties": properties,
            "required": ["name"]
        })
    }

    fn bundle(include_display_name: bool) -> Schema {
        let common: Value = serde_json::from_str(COMMON_SCHEMA).unwrap();
        parse_json_schema_bundle(&[common, profile_schema(include_display_name)]).unwrap()
    }

    #[test]
    fn plugs_commit_referenced_schema_versions_and_documents_query_exact_revisions() {
        let directory = tempfile::tempdir().unwrap();
        let mut plugs_repo = Repository::init(directory.path()).unwrap();

        let v1 = bundle(false);
        let v1_revision = {
            plugs_repo.add(&v1).unwrap();
            plugs_repo.commit("profile v1", "plug-author").unwrap()
        };

        let v2 = bundle(true);
        let v2_revision = {
            plugs_repo.add(&v2).unwrap();
            plugs_repo.commit("profile v2", "plug-author").unwrap()
        };
        drop(plugs_repo);

        // DocumentsRepo gets only this query facade, not Panproto's writer API.
        let schema_store = SchemaStore::open(directory.path()).unwrap();
        let read_v1 = schema_store.schema_at(&v1_revision).unwrap();
        let read_v2 = schema_store.schema_at(&v2_revision).unwrap();

        let profile_root = "https://schemas.example/profile.json";
        let address_definition = "https://schemas.example/common.json:$defs/Address";
        assert!(read_v1.entries.iter().any(|entry| entry == profile_root));
        assert!(read_v1.vertices.contains_key(address_definition));
        assert!(
            read_v1
                .edges
                .keys()
                .any(|edge| { edge.kind == "ref" && edge.tgt == address_definition })
        );
        assert!(
            !read_v1
                .vertices
                .contains_key("https://schemas.example/profile.json.displayName")
        );
        assert!(
            read_v2
                .vertices
                .contains_key("https://schemas.example/profile.json.displayName")
        );

        // Panproto records the directed schema migration on the newer commit;
        // SchemaStore exposes it without making the underlying store writable.
        assert!(schema_store.migration_at(&v1_revision).unwrap().is_none());
        let migration = schema_store.migration_at(&v2_revision).unwrap().unwrap();
        assert_eq!(migration.source, hash_schema(&read_v1).unwrap());
        assert_eq!(migration.target, hash_schema(&read_v2).unwrap());
        let _mapping = migration.mapping;
    }
}
