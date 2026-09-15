use super::*;
use kv_raft::storage::MemStorage;
use kv_raft::storage::RaftStorage;
use kv_raft::types::Entry;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Append(u64, u64),
    Truncate(u64),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1u64..5, 1u64..4).prop_map(|(n, t)| Op::Append(n, t)),
        (1u64..20).prop_map(Op::Truncate),
    ]
}

proptest! {
    #[test]
    fn bitcask_and_mem_storage_agree(ops in proptest::collection::vec(op_strategy(), 1..40)) {
        let dir = tempfile::tempdir().unwrap();
        let mut bitcask = BitcaskStorage::open(dir.path()).unwrap();
        let mut mem = MemStorage::default();

        for op in &ops {
            match *op {
                Op::Append(count, term) => {
                    let start = mem.last_index().unwrap() + 1;
                    let entries: Vec<Entry> = (start..start + count)
                        .map(|index| Entry { term, index, command: vec![term as u8] })
                        .collect();
                    let a = bitcask.append(&entries).is_ok();
                    let b = mem.append(&entries).is_ok();
                    prop_assert_eq!(a, b, "append acceptance diverged at {:?}", op);
                }
                Op::Truncate(from) => {
                    bitcask.truncate_suffix(from).unwrap();
                    mem.truncate_suffix(from).unwrap();
                }
            }

            prop_assert_eq!(bitcask.last_index().unwrap(), mem.last_index().unwrap());
            let hi = mem.last_index().unwrap() + 1;
            prop_assert_eq!(bitcask.entries(1, hi).unwrap(), mem.entries(1, hi).unwrap());
        }

        let hi = mem.last_index().unwrap() + 1;
        let expected = mem.entries(1, hi).unwrap();
        drop(bitcask);
        let reopened = BitcaskStorage::open(dir.path()).unwrap();
        prop_assert_eq!(reopened.entries(1, hi).unwrap(), expected);
    }
}
