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

/// One shard's founding voter set, inside that shard's own directory.
///
/// `.placement` records *which map version* this node founded from; this
/// records *what that map said about this shard*. They are separate because
/// the second has to survive the first being superseded: a founded group has
/// no conf entry to replay its membership from, so without this the bootstrap
/// config on reopen is `--peer` — the whole cluster rather than the shard's
/// three replicas, which is a quorum nobody agreed on.
const VOTERS: &str = ".voters";

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

fn voters_path(config: &NodeConfig, shard: ShardId) -> PathBuf {
    config.shard_dir(shard).join(VOTERS)
}

/// Records the voter set `shard`'s group was founded with.
///
/// Fsynced, and written before the group is handed to the driver, for the same
/// reason [`record_founding`] is: a directory that survived a crash its record
/// did not would be reopened with a membership nobody agreed on.
pub fn record_voters(
    config: &NodeConfig,
    shard: ShardId,
    voters: &[kv_raft::NodeId],
) -> anyhow::Result<()> {
    let path = voters_path(config, shard);
    let text = voters.iter().map(|id| id.to_string()).collect::<Vec<_>>().join("\n");
    std::fs::write(&path, format!("{text}\n"))?;
    std::fs::File::open(&path)?.sync_all()?;
    Ok(())
}

/// The voter set `shard` was founded with, or `None` for a shard that was
/// adopted rather than founded — or founded by a binary that predates this
/// file.
///
/// `None` is not an error. An adopted group learns its config from the leader
/// that added it, which is the only correct source for a group this node was
/// admitted to rather than founded.
pub fn founding_voters(
    config: &NodeConfig,
    shard: ShardId,
) -> anyhow::Result<Option<Vec<kv_raft::NodeId>>> {
    let path = voters_path(config, shard);
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let mut voters = Vec::new();
            for field in text.split_whitespace() {
                voters.push(field.parse::<kv_raft::NodeId>().map_err(|e| {
                    anyhow::anyhow!("{} holds {field:?}, not a node id: {e}", path.display())
                })?);
            }
            Ok(Some(voters))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
