//! [`FileGrantStore`]: the production [`GrantStore`] — one small, checksummed
//! record per group, replaced atomically on every grant.
//!
//! # Record format (version 1)
//!
//! All integers are big-endian. The file holds exactly one record and nothing
//! else; any other length is corrupt.
//!
//! | Offset | Size | Field |
//! |--------|------|-------|
//! | 0 | 4 | magic `b"GNGS"` |
//! | 4 | 1 | format version, `1` |
//! | 5 | 8 | `epoch` (`u64`) |
//! | 13 | 2 | claimant length `n` (`u16`, `1..=65535`) |
//! | 15 | `n` | claimant [`NodeId`] as UTF-8 |
//! | 15 + `n` | 4 | CRC-32 (IEEE) of bytes `0..15 + n` |
//!
//! The version byte is the migration hook a persisted format keeps: a future
//! layout bumps it and teaches [`FileGrantStore::load`] to read both.
//!
//! # Write protocol
//!
//! [`persist`](GrantStore::persist) writes the record to a sibling temporary
//! file (`<name>.tmp` in the target's own directory), `fsync`s it, renames it
//! over the target, and on Unix `fsync`s the directory so the rename itself is
//! durable. Only then does it return `Ok`. A crash at any point leaves either
//! the previous record or the new one at the target path — never a mix — and
//! the driver had not yet sent the grant the unfinished write was ahead of.

use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use groupnet_core::{NodeId, RecoveredGrant};

use super::GrantStore;

/// First four bytes of every record.
const MAGIC: [u8; 4] = *b"GNGS";
/// The one record layout this build writes and reads.
const VERSION: u8 = 1;
/// Bytes before the claimant: magic, version, epoch, claimant length.
const HEADER_LEN: usize = 4 + 1 + 8 + 2;
/// Trailing checksum bytes.
const CHECKSUM_LEN: usize = 4;
/// Largest well-formed record: a maximal claimant between header and checksum.
const MAX_RECORD_LEN: usize = HEADER_LEN + u16::MAX as usize + CHECKSUM_LEN;

/// A durable [`GrantStore`] backed by one file.
///
/// Give each Quorum group its own path. On boot, call [`load`](Self::load) and
/// pass the result to
/// [`GroupProfile::with_voter_storage`](crate::GroupProfile::with_voter_storage)
/// together with this store, **before** joining the group: the join path is
/// synchronous and holds a lock, so it performs no I/O of its own and cannot
/// read the ledger for you.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use groupnet_runtime::{FileGrantStore, GroupProfile};
/// # fn demo(profile: GroupProfile) -> std::io::Result<GroupProfile> {
/// let store = FileGrantStore::open("/var/lib/node/shard-42.grant")?;
/// let recovered = store.load()?; // fail closed: a corrupt ledger is an error
/// Ok(profile.with_voter_storage(recovered, Arc::new(store)))
/// # }
/// ```
///
/// [`load`](Self::load) answers [`RecoveredGrant::none`] only when the file is
/// absent — the storage's attestation that this voter has never granted.
/// Deleting the file therefore makes the voter amnesiac; do not.
#[derive(Debug)]
pub struct FileGrantStore {
    /// The record's path.
    path: PathBuf,
    /// The sibling every write is staged in before the rename.
    tmp: PathBuf,
    /// The directory holding both, `fsync`ed after each rename on Unix.
    #[cfg_attr(
        not(unix),
        expect(dead_code, reason = "only Unix can fsync a directory")
    )]
    dir: PathBuf,
    /// Serializes writers so one staging file suffices and the last call to
    /// return is the record left on disk.
    write: Mutex<()>,
}

impl FileGrantStore {
    /// A store whose record lives at `path`. Nothing is read or written yet.
    ///
    /// # Errors
    /// `NotFound` if the parent directory does not exist, `InvalidInput` if
    /// `path` names no file or its parent is not a directory.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let Some(name) = path.file_name() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "grant store path names no file",
            ));
        };
        let mut tmp_name = OsString::from(name);
        tmp_name.push(".tmp");
        let dir = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        if !fs::metadata(&dir)?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "grant store parent is not a directory",
            ));
        }
        Ok(Self {
            tmp: dir.join(tmp_name),
            path,
            dir,
            write: Mutex::new(()),
        })
    }

    /// The record's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reads the ledger back: [`RecoveredGrant::none`] when no record exists,
    /// [`RecoveredGrant::granted`] with the last persisted pair otherwise.
    ///
    /// # Errors
    /// Any read error, and `InvalidData` for a torn, truncated, oversized or
    /// otherwise corrupt record — never `none()`, because a voter that cannot
    /// read its ledger cannot attest it never granted.
    pub fn load(&self) -> io::Result<RecoveredGrant> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(RecoveredGrant::none());
            }
            Err(err) => return Err(err),
        };
        let (epoch, claimant) = decode(&bytes)?;
        Ok(RecoveredGrant::granted(epoch, claimant))
    }
}

impl GrantStore for FileGrantStore {
    fn persist(&self, epoch: u64, claimant: &NodeId) -> io::Result<()> {
        let record = encode(epoch, claimant)?;
        // A poisoned lock only means another persist panicked mid-write; the
        // staging file is rewritten from scratch below, so carry on.
        let _guard = self
            .write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        {
            let mut tmp = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.tmp)?;
            tmp.write_all(&record)?;
            tmp.sync_all()?;
        }
        fs::rename(&self.tmp, &self.path)?;
        #[cfg(unix)]
        fs::File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}

/// Encodes one version-1 record.
fn encode(epoch: u64, claimant: &NodeId) -> io::Result<Vec<u8>> {
    let id = claimant.as_str().as_bytes();
    let len = u16::try_from(id.len())
        .ok()
        .filter(|len| *len > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "claimant id must be 1..=65535 bytes",
            )
        })?;
    let mut record = Vec::with_capacity(HEADER_LEN + id.len() + CHECKSUM_LEN);
    record.extend_from_slice(&MAGIC);
    record.push(VERSION);
    record.extend_from_slice(&epoch.to_be_bytes());
    record.extend_from_slice(&len.to_be_bytes());
    record.extend_from_slice(id);
    let checksum = crc32(&record);
    record.extend_from_slice(&checksum.to_be_bytes());
    Ok(record)
}

/// Decodes one record, failing closed on anything but an exact, intact
/// version-1 record.
fn decode(bytes: &[u8]) -> io::Result<(u64, NodeId)> {
    let corrupt = |what: &str| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("grant store record {what}"),
        )
    };
    if bytes.len() < HEADER_LEN + 1 + CHECKSUM_LEN || bytes.len() > MAX_RECORD_LEN {
        return Err(corrupt("has an impossible length"));
    }
    let (body, checksum) = bytes.split_at(bytes.len() - CHECKSUM_LEN);
    let stored = u32::from_be_bytes(checksum.try_into().map_err(|_| corrupt("is truncated"))?);
    if crc32(body) != stored {
        return Err(corrupt("fails its checksum"));
    }
    if body[..4] != MAGIC {
        return Err(corrupt("has a foreign magic"));
    }
    if body[4] != VERSION {
        return Err(corrupt("has an unknown version"));
    }
    let epoch = u64::from_be_bytes(
        body[5..13]
            .try_into()
            .map_err(|_| corrupt("is truncated"))?,
    );
    let len = usize::from(u16::from_be_bytes(
        body[13..15]
            .try_into()
            .map_err(|_| corrupt("is truncated"))?,
    ));
    let id = &body[HEADER_LEN..];
    if len == 0 || id.len() != len {
        return Err(corrupt(
            "has a claimant length that disagrees with its size",
        ));
    }
    let id = std::str::from_utf8(id).map_err(|_| corrupt("names a non-UTF-8 claimant"))?;
    Ok((epoch, NodeId::new(id)))
}

/// CRC-32 (IEEE 802.3, reflected, polynomial `0xEDB88320`). Bitwise: a record
/// is a few dozen bytes written once per grant, so a table buys nothing.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    /// A fresh, empty directory unique to this test, removed on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "groupnet-grant-{tag}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("create scratch dir");
            Self(dir)
        }

        fn store(&self) -> FileGrantStore {
            FileGrantStore::open(self.0.join("grant")).expect("open store")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn crc32_matches_the_ieee_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn an_absent_file_attests_never_granted() {
        let scratch = Scratch::new("absent");
        assert_eq!(scratch.store().load().unwrap(), RecoveredGrant::none());
    }

    #[test]
    fn a_persisted_pair_reads_back_through_a_fresh_store() {
        let scratch = Scratch::new("round-trip");
        scratch.store().persist(7, &NodeId::new("node-b")).unwrap();
        assert_eq!(
            scratch.store().load().unwrap(),
            RecoveredGrant::granted(7, NodeId::new("node-b"))
        );
        assert!(
            !scratch.0.join("grant.tmp").exists(),
            "staging file renamed away"
        );
    }

    #[test]
    fn the_last_write_wins() {
        let scratch = Scratch::new("last-wins");
        let store = scratch.store();
        store.persist(3, &NodeId::new("a")).unwrap();
        store
            .persist(9, &NodeId::new("a-much-longer-claimant"))
            .unwrap();
        store.persist(10, &NodeId::new("c")).unwrap();
        assert_eq!(
            store.load().unwrap(),
            RecoveredGrant::granted(10, NodeId::new("c"))
        );
    }

    #[test]
    fn every_truncation_of_a_record_is_rejected() {
        let scratch = Scratch::new("truncated");
        let store = scratch.store();
        store.persist(u64::MAX, &NodeId::new("node-z")).unwrap();
        let full = fs::read(store.path()).unwrap();
        for cut in 0..full.len() {
            fs::write(store.path(), &full[..cut]).unwrap();
            let err = store.load().expect_err("a torn record must not load");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "cut at {cut}");
        }
    }

    #[test]
    fn every_flipped_bit_and_trailing_byte_is_rejected() {
        let scratch = Scratch::new("corrupt");
        let store = scratch.store();
        store.persist(42, &NodeId::new("node-q")).unwrap();
        let full = fs::read(store.path()).unwrap();
        for index in 0..full.len() {
            for bit in 0..8 {
                let mut bad = full.clone();
                bad[index] ^= 1 << bit;
                fs::write(store.path(), &bad).unwrap();
                assert!(store.load().is_err(), "flip {index}:{bit} loaded");
            }
        }
        let mut long = full.clone();
        long.push(0);
        fs::write(store.path(), &long).unwrap();
        assert!(store.load().is_err(), "trailing garbage loaded");
    }

    #[test]
    fn a_well_checksummed_record_of_another_version_is_rejected() {
        let mut record = encode(1, &NodeId::new("n")).unwrap();
        record.truncate(record.len() - CHECKSUM_LEN);
        record[4] = VERSION + 1;
        let checksum = crc32(&record);
        record.extend_from_slice(&checksum.to_be_bytes());
        assert_eq!(
            decode(&record).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn an_unencodable_claimant_is_refused_before_touching_disk() {
        let scratch = Scratch::new("too-long");
        let store = scratch.store();
        let huge = NodeId::new("x".repeat(usize::from(u16::MAX) + 1));
        assert_eq!(
            store.persist(1, &huge).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(store.load().unwrap(), RecoveredGrant::none());
    }

    #[test]
    fn opening_under_a_missing_directory_fails() {
        let scratch = Scratch::new("missing-dir");
        let err = FileGrantStore::open(scratch.0.join("absent").join("grant")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
