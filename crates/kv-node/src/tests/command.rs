use crate::command::{Command, Mutation};
use crate::session::RequestCtx;

fn put(key: &[u8], value: &[u8]) -> Command {
    Command::new(None, Mutation::Put { key: key.to_vec(), value: value.to_vec() })
}

#[test]
fn put_round_trips() {
    let cmd = put(b"k", b"v");
    assert_eq!(Command::decode(&cmd.encode()).unwrap(), Some(cmd));
}

#[test]
fn delete_round_trips() {
    let cmd = Command::new(None, Mutation::Delete { key: b"k".to_vec() });
    assert_eq!(Command::decode(&cmd.encode()).unwrap(), Some(cmd));
}

#[test]
fn cas_round_trips_including_the_absent_expectation() {
    // `expected: None` means "only if absent" — a distinct operation from
    // expecting an empty value, so the two must not collapse.
    let absent = Command::new(
        None,
        Mutation::Cas { key: b"k".to_vec(), expected: None, new_value: b"v".to_vec() },
    );
    let empty = Command::new(
        None,
        Mutation::Cas { key: b"k".to_vec(), expected: Some(Vec::new()), new_value: b"v".to_vec() },
    );
    assert_eq!(Command::decode(&absent.encode()).unwrap(), Some(absent.clone()));
    assert_eq!(Command::decode(&empty.encode()).unwrap(), Some(empty.clone()));
    assert_ne!(absent, empty, "'if absent' and 'if empty' must stay different operations");
}

#[test]
fn the_request_context_survives_the_log() {
    // It rides in the entry because the session table it feeds is replicated
    // state: a context that never reached the log could not be replayed on
    // the node that takes over.
    let cmd = Command::new(
        Some(RequestCtx { client_id: 9, sequence: 4 }),
        Mutation::Delete { key: b"k".to_vec() },
    );
    let back = Command::decode(&cmd.encode()).unwrap().unwrap();
    assert_eq!(back.ctx, Some(RequestCtx { client_id: 9, sequence: 4 }));
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
    let empty_put = put(b"", b"");
    assert!(!empty_put.encode().is_empty(), "an empty put must not collide with the no-op");
    let empty_delete = Command::new(None, Mutation::Delete { key: Vec::new() });
    assert!(!empty_delete.encode().is_empty());
    assert_eq!(Command::decode(&empty_put.encode()).unwrap(), Some(empty_put));
}

#[test]
fn garbage_is_an_error_not_a_silent_no_op() {
    assert!(Command::decode(&[0xff, 0xff, 0xff, 0xff, 0xff]).is_err());
}

/// Membership entries share the log with commands, told apart by magic prefix
/// (M9). That routing is load-bearing — a command misread as conf would move
/// the quorum, and conf misread as command would hit the state machine — so
/// the non-collision is pinned here, next to the no-op pins above: every real
/// command starts with the `ctx: Option` tag byte, `0x00` or `0x01`, which the
/// `RCF9` magic never starts with.
#[test]
fn no_real_command_collides_with_a_conf_entry() {
    use kv_raft::membership::{CONF_MAGIC, decode_conf};

    let cmds = vec![
        put(b"k", b"v"),
        put(b"", b""),
        Command::new(None, Mutation::Delete { key: b"k".to_vec() }),
        Command::new(
            Some(RequestCtx { client_id: 7, sequence: 3 }),
            Mutation::Cas { key: b"k".to_vec(), expected: None, new_value: b"v".to_vec() },
        ),
    ];
    for cmd in &cmds {
        let bytes = cmd.encode();
        assert!(
            bytes.first().is_some_and(|b| *b == 0x00 || *b == 0x01),
            "first byte is the Option tag: {bytes:?}"
        );
        assert!(!bytes.starts_with(&CONF_MAGIC), "command must not carry the conf magic");
        assert!(decode_conf(&bytes).is_none(), "a command must never decode as conf");
    }
}
