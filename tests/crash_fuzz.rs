mod common;

use std::collections::BTreeMap;
use std::ops::Bound::Unbounded;

use common::{Rng, TempDir};
use pagedb::{Database, Error, Options};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

// Random committed / rolled-back / abandoned transactions, then a power cut,
// then recovery; repeated. The database must always equal the model of what
// was committed, whatever the pool size or checkpoint timing was.
fn run(seed: u64, rounds: usize, pool: usize) {
    let dir = TempDir::new("crashfuzz");
    let mut r = Rng(seed | 1);
    let mut model = Model::new();
    let opts = Options { pool_pages: pool, checkpoint_bytes: 256 << 10, ..Options::default() };

    for round in 0..rounds {
        let db = Database::open(dir.path(), opts.clone()).unwrap();
        if round == 0 {
            db.create_table("t").unwrap();
        }
        let t = db.table("t").unwrap();

        let mut got = BTreeMap::new();
        let mut tx = db.begin();
        for (k, v) in tx.scan(&t, Unbounded, Unbounded).unwrap() {
            got.insert(k, v);
        }
        tx.commit().unwrap();
        assert_eq!(got, model, "round {round} seed {seed}: state after recovery differs from model");
        db.verify().unwrap();

        let mut abandoned = vec![];
        for _ in 0..(5 + r.below(25)) {
            let mut tx = db.begin();
            let mut local = model.clone();
            for _ in 0..r.below(60) {
                let key = format!("k{:04}", r.below(700)).into_bytes();
                match r.below(6) {
                    0..=2 => {
                        let n = r.below(300) as usize;
                        let v = r.bytes(n);
                        tx.put(&t, &key, &v).unwrap();
                        local.insert(key, v);
                    }
                    3 => {
                        let had = tx.delete(&t, &key).unwrap();
                        assert_eq!(had, local.remove(&key).is_some());
                    }
                    _ => match tx.insert(&t, &key, b"fresh") {
                        Ok(()) => {
                            local.insert(key, b"fresh".to_vec());
                        }
                        Err(Error::DuplicateKey) => assert!(local.contains_key(&key)),
                        Err(e) => panic!("{e}"),
                    },
                }
            }
            match r.below(6) {
                0 => tx.rollback().unwrap(),
                // left in flight when the power goes; its locks stay held, so it
                // must be the last thing to touch whatever it touched
                1 => {
                    abandoned.push(tx);
                    break;
                }
                _ => {
                    tx.commit().unwrap();
                    model = local;
                }
            }
        }
        if r.below(3) == 0 {
            db.checkpoint().unwrap();
        }
        db.simulate_crash();
        drop(abandoned);
    }
}

#[test]
fn crash_fuzz_small_pool() {
    for seed in [3, 11, 29] {
        run(seed, 12, 20);
    }
}

#[test]
fn crash_fuzz_roomy_pool() {
    for seed in [5, 17] {
        run(seed, 12, 512);
    }
}
