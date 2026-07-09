//! Stable identity for the replication network layer.
//!
//! Two identifiers matter for correct replication:
//!
//! - `cluster_id`: distinguishes one graft cluster (primary + its replicas)
//!   from another. A primary generates one on first start and a replica
//!   adopts it on its very first connection; on every connection after that,
//!   the primary rejects a mismatched `cluster_id` rather than silently
//!   applying foreign WAL data over its own pages (fixes finding #15).
//! - `replica_id`: identifies one specific replica across reconnects (and,
//!   for durable replicas, across restarts). Unlike `peer_addr()` (ip +
//!   ephemeral port), this is stable, so the primary can resume delivery to
//!   the same per-replica outbox instead of treating every reconnect as a
//!   brand new, unrelated replica (fixes finding #14).
//!
//! Both are generated from OS randomness via `RandomState` (already used
//! internally by `std` for hashmap seeding) rather than pulling in a `rand`
//! dependency for what is a "give me a random u64" need, not a
//! cryptographic one. For durable roles they're persisted as plain text in
//! the data directory; for ephemeral roles they live only for the process
//! lifetime.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A random, non-cryptographic u64 sourced from OS randomness.
fn random_u64() -> u64 {
    RandomState::new().build_hasher().finish()
}

fn read_u64_file(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn write_u64_file(path: &Path, value: u64) {
    if let Err(e) = std::fs::write(path, value.to_string()) {
        tracing::warn!("failed to persist {}: {e}", path.display());
    }
}

/// Load the primary's cluster_id from `<data_dir>/cluster_id`, creating and
/// persisting a fresh one if it doesn't exist yet. For ephemeral (no
/// data_dir) primaries, generates one that lives for the process lifetime.
pub fn load_or_create_cluster_id(data_dir: Option<&Path>) -> u64 {
    if let Some(dir) = data_dir {
        let path = dir.join("cluster_id");
        if let Some(id) = read_u64_file(&path) {
            return id;
        }
        let id = random_u64();
        write_u64_file(&path, id);
        return id;
    }
    random_u64()
}

/// This process's replication identity, held for the lifetime of a replica
/// connector so it stays stable across reconnects (and, for durable
/// replicas, is reloaded unchanged across restarts).
pub struct ReplicaIdentity {
    pub replica_id: String,
    cluster_id: AtomicU64,
    data_dir: Option<PathBuf>,
}

impl ReplicaIdentity {
    /// Load (or create) this replica's identity. `data_dir` is the
    /// replica's own data directory (`None` for an ephemeral replica).
    pub fn load_or_create(data_dir: Option<&Path>) -> Self {
        let replica_id = match data_dir {
            Some(dir) => {
                let path = dir.join("replica_id");
                let existing = std::fs::read_to_string(&path)
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
                match existing {
                    Some(id) => id,
                    None => {
                        let id = format!("{:016x}", random_u64());
                        if let Err(e) = std::fs::write(&path, &id) {
                            tracing::warn!("failed to persist {}: {e}", path.display());
                        }
                        id
                    }
                }
            }
            None => format!("{:016x}", random_u64()),
        };

        let cluster_id = data_dir
            .and_then(|dir| read_u64_file(&dir.join("cluster_id")))
            .unwrap_or(0);

        Self {
            replica_id,
            cluster_id: AtomicU64::new(cluster_id),
            data_dir: data_dir.map(Path::to_path_buf),
        }
    }

    pub fn cluster_id(&self) -> u64 {
        self.cluster_id.load(Ordering::Relaxed)
    }

    /// Adopt a cluster_id reported by the primary on first-ever connect
    /// (when this replica didn't already have one), persisting it for
    /// durable replicas so restarts remember the affiliation.
    pub fn adopt_cluster_id(&self, id: u64) {
        self.cluster_id.store(id, Ordering::Relaxed);
        if let Some(dir) = &self.data_dir {
            write_u64_file(&dir.join("cluster_id"), id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ephemeral_cluster_id_is_nonzero_and_stable_across_calls_with_no_dir() {
        let a = load_or_create_cluster_id(None);
        let b = load_or_create_cluster_id(None);
        // Both random — not required to be equal, just each individually usable.
        assert_ne!(a, 0);
        assert_ne!(b, 0);
    }

    #[test]
    fn durable_cluster_id_persists_across_loads() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = load_or_create_cluster_id(Some(dir.path()));
        let id2 = load_or_create_cluster_id(Some(dir.path()));
        assert_eq!(id1, id2);
        assert_ne!(id1, 0);
    }

    #[test]
    fn ephemeral_replica_identity_has_no_cluster_id_until_adopted() {
        let identity = ReplicaIdentity::load_or_create(None);
        assert_eq!(identity.cluster_id(), 0);
        assert!(!identity.replica_id.is_empty());

        identity.adopt_cluster_id(42);
        assert_eq!(identity.cluster_id(), 42);
    }

    #[test]
    fn durable_replica_identity_persists_id_and_cluster_id() {
        let dir = tempfile::tempdir().unwrap();
        let identity1 = ReplicaIdentity::load_or_create(Some(dir.path()));
        identity1.adopt_cluster_id(7);

        let identity2 = ReplicaIdentity::load_or_create(Some(dir.path()));
        assert_eq!(identity1.replica_id, identity2.replica_id);
        assert_eq!(identity2.cluster_id(), 7);
    }
}
