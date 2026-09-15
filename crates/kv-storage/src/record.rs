// crc + timestamp + flags + key_len + value_len
const HEADER_LEN: usize = 4 + 8 + 1 + 4 + 4;
const FLAG_TOMBSTONE: u8 = 0x01;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CodecError {
    #[error("truncated record: need at least {needed} bytes, got {got}")]
    Truncated { needed: usize, got: usize },
    #[error("checksum mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    ChecksumMismatch { expected: u32, computed: u32 },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Record {
    crc: u32,
    timestamp: u64,
    pub(crate) is_tombstone: bool,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

/// CRC32 over everything the header covers except the CRC field itself:
/// timestamp | flags | key_len | value_len | key | value.
fn checksum(timestamp: u64, is_tombstone: bool, key: &[u8], value: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&timestamp.to_be_bytes());
    hasher.update(&[is_tombstone as u8]);
    hasher.update(&(key.len() as u32).to_be_bytes());
    hasher.update(&(value.len() as u32).to_be_bytes());
    hasher.update(key);
    hasher.update(value);
    hasher.finalize()
}

impl Record {
    /// Test-only: constructs a record with a caller-chosen (possibly wrong)
    #[cfg(test)]
    fn new(crc: u32, timestamp: u64, is_tombstone: bool, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { crc, timestamp, is_tombstone, key, value }
    }

    /// Builds a record with a correctly computed checksum. This is the
    /// constructor real callers should use;
    pub(crate) fn create(timestamp: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        let crc = checksum(timestamp, false, &key, &value);
        Self { crc, timestamp, is_tombstone: false, key, value }
    }

    /// Builds a delete marker: no value is stored, and replay (M1.3) removes
    /// `key` from the keydir instead of inserting it.
    pub(crate) fn tombstone(timestamp: u64, key: Vec<u8>) -> Self {
        let value = Vec::new();
        let crc = checksum(timestamp, true, &key, &value);
        Self { crc, timestamp, is_tombstone: true, key, value }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let crc_bytes = self.crc.to_be_bytes();
        let timestamp_bytes = self.timestamp.to_be_bytes();
        let flags = if self.is_tombstone { FLAG_TOMBSTONE } else { 0 };
        let len_key: [u8; 4] = (self.key.len() as u32).to_be_bytes();
        let len_value: [u8; 4] = (self.value.len() as u32).to_be_bytes();
        let mut res =
            [&crc_bytes[..], &timestamp_bytes[..], &[flags][..], &len_key[..], &len_value[..]]
                .concat();
        res.append(&mut self.key.clone());
        res.append(&mut self.value.clone());
        res
    }

    /// Decodes one record from the front of `data`. Returns the record and
    /// the number of bytes it consumed, so callers can decode a stream of
    /// back-to-back records without knowing record boundaries up front.
    pub(crate) fn decode(data: &[u8]) -> Result<(Self, usize), CodecError> {
        if data.len() < HEADER_LEN {
            return Err(CodecError::Truncated { needed: HEADER_LEN, got: data.len() });
        }

        let crc = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let timestamp = u64::from_be_bytes(data[4..12].try_into().unwrap());
        let is_tombstone = data[12] & FLAG_TOMBSTONE != 0;
        let key_len = u32::from_be_bytes(data[13..17].try_into().unwrap()) as usize;
        let value_len = u32::from_be_bytes(data[17..21].try_into().unwrap()) as usize;

        let total_len = HEADER_LEN + key_len + value_len;
        if data.len() < total_len {
            return Err(CodecError::Truncated { needed: total_len, got: data.len() });
        }

        let key = data[HEADER_LEN..HEADER_LEN + key_len].to_vec();
        let value = data[HEADER_LEN + key_len..total_len].to_vec();

        let computed = checksum(timestamp, is_tombstone, &key, &value);
        if computed != crc {
            return Err(CodecError::ChecksumMismatch { expected: crc, computed });
        }

        Ok((Self { crc, timestamp, is_tombstone, key, value }, total_len))
    }
}

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
