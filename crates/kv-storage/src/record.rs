const HEADER_LEN: usize = 4 + 8 + 4 + 4; // crc + timestamp + key_len + value_len

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
    key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

/// CRC32 over everything the header covers except the CRC field itself:
/// timestamp | key_len | value_len | key | value.
fn checksum(timestamp: u64, key: &[u8], value: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&timestamp.to_be_bytes());
    hasher.update(&(key.len() as u32).to_be_bytes());
    hasher.update(&(value.len() as u32).to_be_bytes());
    hasher.update(key);
    hasher.update(value);
    hasher.finalize()
}

impl Record {
    /// Test-only: constructs a record with a caller-chosen (possibly wrong)
    /// crc, for tests that need to check raw byte layout or corruption
    /// handling independent of `create`'s checksum computation.
    #[cfg(test)]
    fn new(crc: u32, timestamp: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { crc, timestamp, key, value }
    }

    /// Builds a record with a correctly computed checksum. This is the
    /// constructor real callers should use; `new` stays available for tests
    /// that deliberately need to construct a record with a specific
    /// (possibly wrong) crc.
    pub(crate) fn create(timestamp: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        let crc = checksum(timestamp, &key, &value);
        Self { crc, timestamp, key, value }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let crc_bytes = self.crc.to_be_bytes();
        let timestamp_bytes = self.timestamp.to_be_bytes();
        let len_key: [u8; 4] = (self.key.len() as u32).to_be_bytes();
        let len_value: [u8; 4] = (self.value.len() as u32).to_be_bytes();
        let mut res = [&crc_bytes[..], &timestamp_bytes[..], &len_key[..], &len_value[..]].concat();
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
        let key_len = u32::from_be_bytes(data[12..16].try_into().unwrap()) as usize;
        let value_len = u32::from_be_bytes(data[16..20].try_into().unwrap()) as usize;

        let total_len = HEADER_LEN + key_len + value_len;
        if data.len() < total_len {
            return Err(CodecError::Truncated { needed: total_len, got: data.len() });
        }

        let key = data[HEADER_LEN..HEADER_LEN + key_len].to_vec();
        let value = data[HEADER_LEN + key_len..total_len].to_vec();

        let computed = checksum(timestamp, &key, &value);
        if computed != crc {
            return Err(CodecError::ChecksumMismatch { expected: crc, computed });
        }

        Ok((Self { crc, timestamp, key, value }, total_len))
    }
}

#[test]
fn encode_record() {
    let key = b"foo".to_vec();
    let value = b"bard".to_vec();
    let record = Record::new(0xDEADBEEF, 42, key.clone(), value.clone());

    let encoded = record.encode();

    let mut expected = Vec::new();
    expected.extend_from_slice(&0xDEADBEEFu32.to_be_bytes());
    expected.extend_from_slice(&42u64.to_be_bytes());
    expected.extend_from_slice(&(key.len() as u32).to_be_bytes());
    expected.extend_from_slice(&(value.len() as u32).to_be_bytes());
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&value);

    assert_eq!(encoded, expected);
}

#[test]
fn create_computes_matching_checksum() {
    let key = b"foo".to_vec();
    let value = b"bard".to_vec();
    let record = Record::create(42, key.clone(), value.clone());

    assert_eq!(record.crc, checksum(42, &key, &value));
}

#[test]
fn decode_inverts_encode() {
    let key = b"foo".to_vec();
    let value = b"bard".to_vec();
    let record = Record::create(42, key, value);

    let encoded = record.encode();
    let (decoded, consumed) = Record::decode(&encoded).unwrap();

    assert_eq!(decoded, record);
    assert_eq!(consumed, encoded.len());
}

#[test]
fn decode_rejects_truncated_header() {
    let err = Record::decode(&[0u8; 10]).unwrap_err();
    assert!(matches!(err, CodecError::Truncated { needed: HEADER_LEN, got: 10 }));
}

#[test]
fn decode_rejects_truncated_body() {
    let key = b"foo".to_vec();
    let value = b"bard".to_vec();
    let record = Record::create(42, key, value);
    let mut encoded = record.encode();
    encoded.truncate(encoded.len() - 1); // drop the last byte of `value`

    let err = Record::decode(&encoded).unwrap_err();
    assert!(matches!(err, CodecError::Truncated { .. }));
}

#[test]
fn decode_detects_corrupted_byte() {
    let key = b"foo".to_vec();
    let value = b"bard".to_vec();
    let record = Record::create(42, key, value);
    let mut encoded = record.encode();

    // Flip a bit inside `value`, after the checksum field itself.
    let last = encoded.len() - 1;
    encoded[last] ^= 0x01;

    let err = Record::decode(&encoded).unwrap_err();
    assert!(matches!(err, CodecError::ChecksumMismatch { .. }));
}
