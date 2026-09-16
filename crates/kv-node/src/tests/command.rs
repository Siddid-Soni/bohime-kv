use crate::command::Command;

#[test]
fn put_round_trips() {
    let cmd = Command::Put { key: b"k".to_vec(), value: b"v".to_vec() };
    assert_eq!(Command::decode(&cmd.encode()).unwrap(), Some(cmd));
}

#[test]
fn delete_round_trips() {
    let cmd = Command::Delete { key: b"k".to_vec() };
    assert_eq!(Command::decode(&cmd.encode()).unwrap(), Some(cmd));
}

/// The leader's no-op entry carries an empty command. It reaches the apply
/// path like any other committed entry, so it must decode as "nothing to do".
/// Treating it as an error would make every election poison the state machine.
#[test]
fn the_leaders_no_op_decodes_as_nothing() {
    assert_eq!(Command::decode(&[]).unwrap(), None);
}

/// The neighbouring trap: no real command may ever encode to zero bytes, or
/// it would be indistinguishable from the no-op and silently dropped.
#[test]
fn no_real_command_encodes_to_nothing() {
    let empty_put = Command::Put { key: Vec::new(), value: Vec::new() };
    assert!(!empty_put.encode().is_empty(), "an empty put must not collide with the no-op");
    let empty_delete = Command::Delete { key: Vec::new() };
    assert!(!empty_delete.encode().is_empty());
    assert_eq!(Command::decode(&empty_put.encode()).unwrap(), Some(empty_put));
}

#[test]
fn garbage_is_an_error_not_a_silent_no_op() {
    assert!(Command::decode(&[0xff, 0xff, 0xff, 0xff, 0xff]).is_err());
}
