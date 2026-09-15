//! File-backed snapshot store. Each snapshot lives in its own directory
//! under `<base>/snapshots/<term>-<index>-<msec>/`. The state file is
//! `state.bin`; metadata (including the CRC64 checksum) is in
//! `meta.json`.
//!
//! Mirrors `file_snapshot.go` of the Go implementation. Only snapshot
//! version 1 is supported.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::configuration::Configuration;
use crate::snapshot::{snapshot_name, SnapshotMeta, SnapshotReader, SnapshotSink, SnapshotStore};
use crate::{RaftError, Result};

const SNAPSHOTS_SUBDIR: &str = "snapshots";
const META_FILE: &str = "meta.json";
const STATE_FILE: &str = "state.bin";
const TMP_SUFFIX: &str = ".tmp";

/// File-backed [`SnapshotStore`] that retains up to `retain` snapshots.
/// Mirrors the Go `FileSnapshotStore`.
pub struct FileSnapshotStore {
    path: PathBuf,
    retain: usize,
    /// Skip `fsync` and `dir.sync`. Mirrors `noSync` from the Go
    /// implementation; only intended for tests.
    no_sync: bool,
}

impl FileSnapshotStore {
    /// Creates a new `FileSnapshotStore` rooted at `base/snapshots/`.
    /// `retain` controls how many snapshots are kept on disk.
    pub fn new(base: impl AsRef<Path>, retain: usize) -> Result<Self> {
        if retain < 1 {
            return Err(RaftError::Other("must retain at least one snapshot".into()));
        }
        let path = base.as_ref().join(SNAPSHOTS_SUBDIR);
        fs::create_dir_all(&path)
            .map_err(|e| RaftError::Snapshot(format!("snapshot path not accessible: {}", e)))?;
        Ok(FileSnapshotStore {
            path,
            retain,
            no_sync: false,
        })
    }

    /// Disable `fsync`/directory-sync calls. Test-only escape hatch
    /// mirroring the Go `noSync` flag.
    pub fn set_no_sync(&mut self, no_sync: bool) {
        self.no_sync = no_sync;
    }

    /// Path to the snapshot directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Sort comparator matching Go's `snapMetaSlice.Less`: term, index,
    /// then id.
    fn cmp(a: &FileSnapshotMeta, b: &FileSnapshotMeta) -> std::cmp::Ordering {
        a.term
            .cmp(&b.term)
            .then_with(|| a.index.cmp(&b.index))
            .then_with(|| a.id.cmp(&b.id))
    }

    fn collect_snapshots(&self) -> Result<Vec<FileSnapshotMeta>> {
        let entries = fs::read_dir(&self.path)
            .map_err(|e| RaftError::Snapshot(format!("scan snapshot directory: {}", e)))?;
        let mut out = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let name = entry.file_name();
            let name = name.to_string_lossy().to_string();
            // Skip files and any temporary directories.
            if !entry.path().is_dir() || name.ends_with(TMP_SUFFIX) {
                continue;
            }
            match self.read_meta(&name) {
                Ok(meta) => out.push(meta),
                Err(_) => continue,
            }
        }
        Ok(out)
    }

    fn read_meta(&self, name: &str) -> Result<FileSnapshotMeta> {
        let meta_path = self.path.join(name).join(META_FILE);
        let file = File::open(&meta_path).map_err(|e| {
            RaftError::Snapshot(format!("open meta {}: {}", meta_path.display(), e))
        })?;
        let reader = BufReader::new(file);
        let meta: FileSnapshotMeta = serde_json::from_reader(reader)
            .map_err(|e| RaftError::Snapshot(format!("parse meta: {}", e)))?;
        Ok(meta)
    }

    /// Reap snapshots beyond the retain count. Mirrors
    /// `FileSnapshotStore.reapSnapshots`.
    pub fn reap(&self) -> Result<()> {
        let mut snapshots = self.collect_snapshots()?;
        snapshots.sort_by(|a, b| Self::cmp(a, b).reverse());
        for meta in snapshots.iter().skip(self.retain) {
            let path = self.path.join(&meta.id);
            fs::remove_dir_all(&path)
                .map_err(|e| RaftError::Snapshot(format!("reap {}: {}", path.display(), e)))?;
        }
        Ok(())
    }
}

#[async_trait]
impl SnapshotStore for FileSnapshotStore {
    async fn create(
        &self,
        version: u8,
        index: u64,
        term: u64,
        configuration: &Configuration,
        configuration_index: u64,
    ) -> Result<Box<dyn SnapshotSink>> {
        if version != 1 {
            return Err(RaftError::Snapshot(format!(
                "unsupported snapshot version {}",
                version
            )));
        }

        let id = snapshot_name(term, index);
        let tmp_dir = self.path.join(format!("{}{}", id, TMP_SUFFIX));
        fs::create_dir_all(&tmp_dir)
            .map_err(|e| RaftError::Snapshot(format!("create tmp dir: {}", e)))?;

        let meta = FileSnapshotMeta {
            version,
            id: id.clone(),
            index,
            term,
            configuration: configuration.clone(),
            configuration_index,
            size: 0,
            crc: None,
        };

        let sink = FileSnapshotSink::new(self, tmp_dir, meta)?;
        Ok(Box::new(sink))
    }

    async fn list(&self) -> Result<Vec<SnapshotMeta>> {
        let mut snapshots = self.collect_snapshots()?;
        snapshots.sort_by(|a, b| Self::cmp(a, b).reverse());
        Ok(snapshots
            .into_iter()
            .take(self.retain)
            .map(|m| m.into_public())
            .collect())
    }

    async fn open(&self, id: &str) -> Result<(SnapshotMeta, SnapshotReader)> {
        let meta = self.read_meta(id)?;
        let state_path = self.path.join(id).join(STATE_FILE);
        let file = File::open(&state_path).map_err(|e| {
            RaftError::Snapshot(format!("open state file {}: {}", state_path.display(), e))
        })?;

        // Verify the CRC64 of the state file.
        let mut hasher = crc64::Crc64::new();
        let mut reader = BufReader::new(&file);
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let computed = hasher.finish().into_bytes();
        match &meta.crc {
            Some(stored) if stored.as_slice() != &computed[..] => {
                return Err(RaftError::Snapshot("CRC mismatch".into()));
            }
            _ => {}
        }

        // Hand back a fresh, buffered reader positioned at the start of
        // the file (we already consumed it for CRC verification).
        let file = File::open(&state_path).map_err(|e| {
            RaftError::Snapshot(format!("reopen state file {}: {}", state_path.display(), e))
        })?;
        let reader: SnapshotReader = Box::new(BufReader::new(file));
        Ok((meta.into_public(), reader))
    }
}

/// On-disk metadata for a snapshot: the public [`SnapshotMeta`] fields plus
/// the CRC64 of the state file. Mirrors the Go `fileSnapshotMeta`. The
/// fields are flattened in serialization so the on-disk format is exactly
/// `<SnapshotMeta fields>` plus an optional `crc` key.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileSnapshotMeta {
    pub version: u8,
    pub id: String,
    pub index: u64,
    pub term: u64,
    pub configuration: Configuration,
    pub configuration_index: u64,
    pub size: u64,
    /// CRC64-ECMA checksum of the state file. Populated on close.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc: Option<Vec<u8>>,
}

impl FileSnapshotMeta {
    fn into_public(self) -> SnapshotMeta {
        SnapshotMeta {
            version: self.version,
            id: self.id,
            index: self.index,
            term: self.term,
            configuration: self.configuration,
            configuration_index: self.configuration_index,
            size: self.size,
        }
    }
}

/// [`SnapshotSink`] backed by a temp directory. On close the directory is
/// renamed into place; on cancel it is removed.
struct FileSnapshotSink {
    store: *const FileSnapshotStore,
    dir: PathBuf,
    meta: FileSnapshotMeta,
    state_file: Option<File>,
    closed: bool,
}

impl FileSnapshotSink {
    fn new(store: &FileSnapshotStore, dir: PathBuf, meta: FileSnapshotMeta) -> Result<Self> {
        let state_path = dir.join(STATE_FILE);
        let file = File::create(&state_path).map_err(|e| {
            RaftError::Snapshot(format!("create state file {}: {}", state_path.display(), e))
        })?;
        Ok(FileSnapshotSink {
            store: store as *const _,
            dir,
            meta,
            state_file: Some(file),
            closed: false,
        })
    }

    fn write_meta(&self) -> Result<()> {
        let meta_path = self.dir.join(META_FILE);
        let file = File::create(&meta_path).map_err(|e| {
            RaftError::Snapshot(format!("create meta file {}: {}", meta_path.display(), e))
        })?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, &self.meta)
            .map_err(|e| RaftError::Snapshot(format!("write meta: {}", e)))?;
        writer.flush()?;

        let no_sync = unsafe { (*self.store).no_sync };
        if !no_sync {
            let inner = writer
                .into_inner()
                .map_err(|e| RaftError::Snapshot(format!("flush meta: {}", e)))?;
            inner.sync_data()?;
        }
        Ok(())
    }

    fn rename_into_place(&self) -> Result<()> {
        let trimmed = self
            .dir
            .to_string_lossy()
            .trim_end_matches(TMP_SUFFIX)
            .to_string();
        let new_path = PathBuf::from(trimmed);
        fs::rename(&self.dir, &new_path).map_err(|e| {
            RaftError::Snapshot(format!(
                "rename {} -> {}: {}",
                self.dir.display(),
                new_path.display(),
                e
            ))
        })?;
        Ok(())
    }
}

// SAFETY: the `*const FileSnapshotStore` is only ever dereferenced inside
// `async_trait` methods. Those methods are called by the raft main loop on
// a single task at a time (the sink is not shared across threads), so
// adding Send/Sync is sound.
unsafe impl Send for FileSnapshotSink {}
unsafe impl Sync for FileSnapshotSink {}

#[async_trait]
impl SnapshotSink for FileSnapshotSink {
    fn id(&self) -> String {
        self.meta.id.clone()
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let file = self
            .state_file
            .as_mut()
            .expect("state file present until close");
        file.write_all(buf)?;
        Ok(buf.len())
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        // Finalize the state file: capture size + CRC.
        let mut file = self
            .state_file
            .take()
            .expect("state file present until close");

        let no_sync = unsafe { (*self.store).no_sync };
        if !no_sync {
            file.flush()?;
            file.sync_data()?;
        }

        let metadata = file.metadata()?;
        self.meta.size = metadata.len();

        // Compute the CRC over the just-written state file.
        let mut hasher = crc64::Crc64::new();
        let state_path = self.dir.join(STATE_FILE);
        let mut reader = BufReader::new(File::open(&state_path)?);
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        self.meta.crc = Some(hasher.finish().into_bytes().to_vec());

        drop(file);

        // Persist metadata and rename into place.
        self.write_meta()?;
        self.rename_into_place()?;

        // SAFETY: only used within this method's scope.
        let store = unsafe { &*self.store };
        store.reap()?;
        Ok(())
    }

    async fn cancel(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        // Drop the file handle first so we can delete the directory.
        self.state_file.take();
        fs::remove_dir_all(&self.dir)
            .map_err(|e| RaftError::Snapshot(format!("cancel snapshot: {}", e)))?;
        Ok(())
    }
}

/// Tiny CRC64-ECMA implementation. Mirrors `crc64.New(crc64.MakeTable(crc64.ECMA))`.
mod crc64 {
    pub struct Crc64 {
        table: [u64; 256],
        value: u64,
    }
    impl Crc64 {
        pub fn new() -> Self {
            // ECMA polynomial: 0xC96C5795D7870F42 (reversed: 0x42F0E1EBA9EA3693).
            const POLY: u64 = 0x42F0E1EBA9EA3693;
            let mut table = [0u64; 256];
            for i in 0..256u64 {
                let mut crc = i;
                for _ in 0..8 {
                    if crc & 1 != 0 {
                        crc = (crc >> 1) ^ POLY;
                    } else {
                        crc >>= 1;
                    }
                }
                table[i as usize] = crc;
            }
            Crc64 { table, value: 0 }
        }
        pub fn update(&mut self, buf: &[u8]) {
            for &b in buf {
                let idx = ((self.value ^ b as u64) & 0xFF) as usize;
                self.value = (self.value >> 8) ^ self.table[idx];
            }
        }
        pub fn finish(&self) -> Crc64Digest {
            Crc64Digest(self.value)
        }
    }
    pub struct Crc64Digest(pub(crate) u64);
    impl Crc64Digest {
        /// Big-endian serialization, matching Go's crc64 package output.
        pub fn into_bytes(self) -> [u8; 8] {
            self.0.to_be_bytes()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_tmp(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "raft-rust-{}-{}-{}",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        path
    }

    fn test_conf() -> Configuration {
        Configuration {
            servers: vec![crate::configuration::Server {
                suffrage: crate::configuration::ServerSuffrage::Voter,
                id: "a".into(),
                address: "addr-a".into(),
            }],
        }
    }

    /// Mirrors `TestFileSS_CreateSnapshot`: write through the sink,
    /// close, list, and open the snapshot to verify the data round-trips.
    #[tokio::test]
    async fn create_write_list_open() {
        let dir = unique_tmp("create");
        let store = FileSnapshotStore::new(&dir, 3).unwrap();

        let mut sink = store.create(1, 10, 3, &test_conf(), 2).await.unwrap();
        let id = sink.id();
        sink.write(b"first ").await.unwrap();
        sink.write(b"second").await.unwrap();
        sink.close().await.unwrap();

        let snaps = store.list().await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].id, id);
        assert_eq!(snaps[0].index, 10);
        assert_eq!(snaps[0].term, 3);
        assert_eq!(snaps[0].size, 12);

        let (meta, mut reader) = store.open(&id).await.unwrap();
        assert_eq!(meta.index, 10);
        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "first second");

        // Cleanup
        let _ = fs::remove_dir_all(&dir);
    }

    /// Cancel discards the temp directory without leaving a snapshot.
    #[tokio::test]
    async fn cancel_discards() {
        let dir = unique_tmp("cancel");
        let store = FileSnapshotStore::new(&dir, 1).unwrap();

        let mut sink = store.create(1, 5, 1, &test_conf(), 1).await.unwrap();
        sink.write(b"data").await.unwrap();
        sink.cancel().await.unwrap();

        let snaps = store.list().await.unwrap();
        assert!(snaps.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    /// `retain` controls how many snapshots are kept; older ones are
    /// reaped.
    #[tokio::test]
    async fn retain_reaps_older() {
        let dir = unique_tmp("retain");
        let store = FileSnapshotStore::new(&dir, 2).unwrap();

        // Take 3 snapshots in order of increasing index.
        for i in 1..=3 {
            let mut sink = store.create(1, i * 10, 1, &test_conf(), i).await.unwrap();
            sink.write(format!("snap-{}", i).as_bytes()).await.unwrap();
            sink.close().await.unwrap();
        }

        let snaps = store.list().await.unwrap();
        // Only the most recent 2 should be retained.
        assert_eq!(snaps.len(), 2);
        let ids: Vec<&str> = snaps.iter().map(|s| s.id.as_str()).collect();
        assert!(!ids.iter().any(|id| id.contains("-10-")));

        let _ = fs::remove_dir_all(&dir);
    }

    /// A snapshot whose state file is corrupted produces a CRC mismatch
    /// on open.
    #[tokio::test]
    async fn corrupted_state_fails_crc() {
        let dir = unique_tmp("crc");
        let store = FileSnapshotStore::new(&dir, 1).unwrap();

        let mut sink = store.create(1, 7, 1, &test_conf(), 1).await.unwrap();
        let id = sink.id();
        sink.write(b"hello").await.unwrap();
        sink.close().await.unwrap();

        // Corrupt the state file.
        let state_path = dir.join(SNAPSHOTS_SUBDIR).join(&id).join(STATE_FILE);
        fs::write(&state_path, b"corrupt").unwrap();

        let err = match store.open(&id).await {
            Ok(_) => panic!("expected CRC error"),
            Err(e) => e,
        };
        assert!(matches!(err, RaftError::Snapshot(ref msg) if msg.contains("CRC")));

        let _ = fs::remove_dir_all(&dir);
    }
}
