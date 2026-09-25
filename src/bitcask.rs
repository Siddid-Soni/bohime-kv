//! A single-file Bitcask: an append-only log of records plus an in-memory
//! keydir mapping each live key to where its value sits in the file.
//!
//! Record: `crc32 | key_len u32 | value_len u32 | key | value`, little endian.
//! A `value_len` of `u32::MAX` marks a tombstone. There is no compaction: the
//! file only grows. Records are buffered in memory until `flush`, so a batch
//! of them costs one write.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

const HEADER: usize = 12;
const TOMBSTONE: u32 = u32::MAX;

/// `(key, value or None for a tombstone, record length)`.
type Record<'a> = (&'a [u8], Option<&'a [u8]>, usize);

pub struct Bitcask {
    file: File,
    keydir: HashMap<Vec<u8>, (u64, u32)>,
    /// Offset just past the last record, buffered ones included.
    end: u64,
    /// Records not yet written; the first sits at `end - buf.len()`.
    buf: Vec<u8>,
}

impl Bitcask {
    /// Replays the file to rebuild the keydir. The first record that fails
    /// its checksum or runs past the end is a torn write from a crash: it and
    /// everything after it are cut off.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let data = std::fs::read(path)?;
        let mut db = Self { file, keydir: HashMap::new(), end: 0, buf: vec![] };
        while let Some((key, value, len)) = decode(&data[db.end as usize..]) {
            db.index(key, value.map(|v| v.len()), len);
        }
        db.file.set_len(db.end)?;
        Ok(db)
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let Some(&(offset, len)) = self.keydir.get(key) else { return Ok(None) };
        let (offset, len) = (offset as usize, len as usize);
        let written = self.end as usize - self.buf.len();
        if offset >= written {
            return Ok(Some(self.buf[offset - written..][..len].to_vec()));
        }
        let mut value = vec![0; len];
        self.file.read_exact_at(&mut value, offset as u64)?;
        Ok(Some(value))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.append(key, Some(value))
    }

    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        self.append(key, None)
    }

    /// Hands the buffered records to the OS in one write.
    pub fn flush(&mut self) -> io::Result<()> {
        let written = self.end - self.buf.len() as u64;
        self.file.write_all_at(&self.buf, written)?;
        self.buf.clear();
        Ok(())
    }

    /// Writes are not durable until this returns.
    pub fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        self.file.sync_data()
    }

    /// A handle to fsync on another thread, after `flush`, while this one
    /// keeps buffering.
    pub fn file(&self) -> io::Result<File> {
        self.file.try_clone()
    }

    fn append(&mut self, key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        let len = self.buf.len();
        encode(&mut self.buf, key, value);
        self.index(key, value.map(|v| v.len()), self.buf.len() - len);
        Ok(())
    }

    fn index(&mut self, key: &[u8], value_len: Option<usize>, record_len: usize) {
        match value_len {
            Some(len) => {
                let offset = self.end + (HEADER + key.len()) as u64;
                self.keydir.insert(key.to_vec(), (offset, len as u32));
            }
            None => {
                self.keydir.remove(key);
            }
        }
        self.end += record_len as u64;
    }
}

/// Appends one record to `buf`.
fn encode(buf: &mut Vec<u8>, key: &[u8], value: Option<&[u8]>) {
    let start = buf.len();
    buf.extend([0; 4]);
    buf.extend((key.len() as u32).to_le_bytes());
    buf.extend(value.map_or(TOMBSTONE, |v| v.len() as u32).to_le_bytes());
    buf.extend(key);
    buf.extend(value.unwrap_or_default());
    let crc = crc32fast::hash(&buf[start + 4..]);
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

/// `None` if `buf` does not start with a whole, intact record.
fn decode(buf: &[u8]) -> Option<Record<'_>> {
    let header = buf.get(..HEADER)?;
    let word = |i: usize| u32::from_le_bytes(header[i..i + 4].try_into().unwrap());
    let (key_len, value_len) = (word(4) as usize, word(8));
    let len = HEADER + key_len + if value_len == TOMBSTONE { 0 } else { value_len as usize };
    if crc32fast::hash(buf.get(4..len)?) != word(0) {
        return None;
    }
    let value = (value_len != TOMBSTONE).then(|| &buf[HEADER + key_len..len]);
    Some((&buf[HEADER..HEADER + key_len], value, len))
}
