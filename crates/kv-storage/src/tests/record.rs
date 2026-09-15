use crate::record::{CodecError, FLAG_TOMBSTONE, HEADER_LEN, Record, checksum};

#[test]
fn encode_record() {
    let key = b"foo".to_vec();
    let value = b"bard".to_vec();
    let record = Record::new(0xDEADBEEF, 42, false, key.clone(), value.clone());

    let encoded = record.encode();

    let mut expected = Vec::new();
    expected.extend_from_slice(&0xDEADBEEFu32.to_be_bytes());
    expected.extend_from_slice(&42u64.to_be_bytes());
    expected.push(0); // flags: not a tombstone
    expected.extend_from_slice(&(key.len() as u32).to_be_bytes());
    expected.extend_from_slice(&(value.len() as u32).to_be_bytes());
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&value);

    assert_eq!(encoded, expected);
}

#[test]
fn encode_tombstone_sets_flag_byte() {
    let record = Record::new(0xDEADBEEF, 42, true, b"foo".to_vec(), Vec::new());

    let encoded = record.encode();

    assert_eq!(encoded[12], FLAG_TOMBSTONE);
}

#[test]
fn create_computes_matching_checksum() {
    let key = b"foo".to_vec();
    let value = b"bard".to_vec();
    let record = Record::create(42, key.clone(), value.clone());

    assert_eq!(record.crc, checksum(42, false, &key, &value));
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
fn tombstone_round_trips() {
    let record = Record::tombstone(42, b"k".to_vec());

    let encoded = record.encode();
    let (decoded, consumed) = Record::decode(&encoded).unwrap();

    assert!(decoded.is_tombstone);
    assert_eq!(decoded.key, b"k".to_vec());
    assert_eq!(decoded.value, Vec::<u8>::new());
    assert_eq!(consumed, encoded.len());
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
