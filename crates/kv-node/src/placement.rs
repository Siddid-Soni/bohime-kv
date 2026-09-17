//! Which shards this node hosts, and the record that makes founding a
//! one-time act (M11.1).
//!
//! Placement itself is replicated — the meta group holds the map, `kv-ring`
//! computes it. What lives here is the *local* half: the directories a node
//! has opened, and the marker saying which map version it opened them from.
//!
//! **Why founding has to be recorded.** Every replica of shard 7 derives the
//! same voter set from the same map version, so a shard group can be founded
//! with no conf change at all. That argument holds only while they agree on
//! the version. A map version that moves one replica slot would have the
//! newcomer found shard 7 with voters `{1,2,4}` while nodes 1 and 2 are
//! already running it with `{1,2,3}` — two configurations for one shard, which
//! is split brain arriving through the front door. So a node founds groups
//! once, from the first map it ever sees, and every later boot hosts exactly
//! what is on disk. Moving a shard to a node that has never held it is a data
//! move, which is M12's migration driver.

use std::collections::BTreeSet;
use std::path::PathBuf;

use kv_ring::ShardId;

use crate::config::NodeConfig;

/// Names the founding map version. Under `shards/` rather than the data dir
/// so that removing `shards/` removes the claim to have founded them too —
/// a half-wiped store is worse than a wiped one.
const MARKER: &str = ".placement";

/// A store written before M11: one Raft log and one state machine holding
/// keys from every shard.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "{path} is a pre-M11 single-group store: one log holding keys from every shard. \
     Splitting it across per-shard groups moves data between Raft groups, which is a \
     migration rather than a startup step — start from an empty --data-dir, or restore \
     the data through a client"
)]
pub struct LegacyLayout {
    pub path: PathBuf,
}

/// Refuses to serve over a store this binary would misread.
///
/// Starting anyway would present an empty database over a directory holding
/// the entire previous dataset, and the operator's next act would be to write
/// into the empty one.
pub fn check_layout(config: &NodeConfig) -> Result<(), LegacyLayout> {
    for path in [config.legacy_raft_dir(), config.legacy_state_dir()] {
        if path.exists() {
            return Err(LegacyLayout { path });
        }
    }
    Ok(())
}

fn marker_path(config: &NodeConfig) -> PathBuf {
    config.shards_dir().join(MARKER)
}

/// The map version this node founded its shard groups at, or `None` if it
/// never has.
pub fn founded_at(config: &NodeConfig) -> anyhow::Result<Option<u64>> {
    let path = marker_path(config);
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let version = text.trim().parse::<u64>().map_err(|e| {
                anyhow::anyhow!("{} does not hold a map version: {e}", path.display())
            })?;
            Ok(Some(version))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Records that this node's shards were founded from map `version`.
///
/// Written after the directories exist, and fsynced: a marker that survived a
/// crash its directories did not would have the node found a second time from
/// a later map, which is the one thing this file exists to prevent. The
/// ordering is the same disk-before-anything-observable rule the driver's
/// drain follows.
pub fn record_founding(config: &NodeConfig, version: u64) -> anyhow::Result<()> {
    let dir = config.shards_dir();
    std::fs::create_dir_all(&dir)?;
    let path = marker_path(config);
    std::fs::write(&path, format!("{version}\n"))?;
    std::fs::File::open(&path)?.sync_all()?;
    Ok(())
}

/// The shards this node already has directories for.
///
/// The truth after a restart, in preference to the map: a group whose log is
/// on this disk is one this node is a member of as far as Raft is concerned,
/// whatever placement has since decided.
pub fn hosted_on_disk(config: &NodeConfig) -> anyhow::Result<BTreeSet<ShardId>> {
    let dir = config.shards_dir();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(e) => return Err(e.into()),
    };

    let mut shards = BTreeSet::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        // Anything that is not a shard number is left alone rather than
        // guessed at: a `lost+found` read as a shard id would become a Raft
        // group nobody placed there.
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Ok(shard) = name.parse::<ShardId>() else { continue };
        shards.insert(shard);
    }
    Ok(shards)
}
