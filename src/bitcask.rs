//! A single-file Bitcask: an append-only log of records plus an in-memory
//! keydir mapping each live key to where its value sits in the file.
//!
//! Record: `crc32 | key_len u32 | value_len u32 | key | value`, little endian.
//! A `value_len` of `u32::MAX` marks a tombstone. There is no compaction: the
//! file only grows.

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
    end: u64,
}

impl Bitcask {
    /// Replays the file to rebuild the keydir. The first record that fails
    /// its checksum or runs past the end is a torn write from a crash: it and
    /// everything after it are cut off.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let data = std::fs::read(path)?;
        let mut db = Self { file, keydir: HashMap::new(), end: 0 };
        while let Some((key, value, len)) = decode(&data[db.end as usize..]) {
            db.index(key, value.map(|v| v.len()), len);
        }
        db.file.set_len(db.end)?;
        Ok(db)
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let Some(&(offset, len)) = self.keydir.get(key) else { return Ok(None) };
        let mut value = vec![0; len as usize];
        self.file.read_exact_at(&mut value, offset)?;
        Ok(Some(value))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.append(key, Some(value))
    }

    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        self.append(key, None)
    }

    /// Writes are not durable until this returns.
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn append(&mut self, key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        let record = encode(key, value);
        self.file.write_all_at(&record, self.end)?;
        self.index(key, value.map(|v| v.len()), record.len());
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

fn encode(key: &[u8], value: Option<&[u8]>) -> Vec<u8> {
    let mut record = vec![0; 4];
    record.extend((key.len() as u32).to_le_bytes());
    record.extend(value.map_or(TOMBSTONE, |v| v.len() as u32).to_le_bytes());
    record.extend(key);
    record.extend(value.unwrap_or_default());
    let crc = crc32fast::hash(&record[4..]);
    record[..4].copy_from_slice(&crc.to_le_bytes());
    record
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
