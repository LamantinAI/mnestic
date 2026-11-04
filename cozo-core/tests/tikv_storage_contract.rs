#![cfg(feature = "storage-tikv")]

use cozo::{
    new_cozo_tikv_with_config, DbInstance, NamedRows, ScriptMutability, Storage, StoreTx,
    TiKvConfig, TiKvStorage, TiKvTlsConfig,
};
use serde_json::json;
use std::{collections::BTreeMap, env};
use uuid::Uuid;

fn config(optimistic: bool) -> TiKvConfig {
    let mut config = TiKvConfig {
        end_points: vec![
            env::var("MNESTIC_TIKV_PD_ENDPOINT").expect("set MNESTIC_TIKV_PD_ENDPOINT")
        ],
        namespace: Some(format!("contract-{}", Uuid::new_v4())),
        optimistic,
        scan_batch_size: 3,
        ..Default::default()
    };
    if let (Ok(ca), Ok(cert), Ok(key)) = (
        env::var("MNESTIC_TIKV_CA"),
        env::var("MNESTIC_TIKV_CERT"),
        env::var("MNESTIC_TIKV_KEY"),
    ) {
        config.tls = Some(TiKvTlsConfig {
            ca: ca.into(),
            cert: cert.into(),
            key: key.into(),
        });
    }
    config
}
fn run(db: &DbInstance, script: &str) -> NamedRows {
    db.run_script(script, BTreeMap::new(), ScriptMutability::Mutable)
        .unwrap()
}

#[test]
#[ignore = "requires a disposable TiKV cluster"]
fn rollback_pending_range_delete_ordered_batch_get_and_full_scan() {
    for optimistic in [true, false] {
        let store = TiKvStorage::connect(config(optimistic)).unwrap();
        let mut seed = store.transact(true).unwrap();
        seed.put(b"a", b"old").unwrap();
        seed.put(b"b", b"remove").unwrap();
        seed.commit().unwrap();
        drop(seed);
        let mut tx = store.transact(true).unwrap();
        tx.put(b"a", b"new").unwrap();
        tx.put(b"c", b"insert").unwrap();
        tx.del_range_from_persisted(b"a", b"z").unwrap();
        let keys = vec![
            b"c".to_vec(),
            b"a".to_vec(),
            b"none".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
        ];
        assert_eq!(
            tx.multi_get(&keys, true).unwrap(),
            vec![
                Some(b"insert".to_vec()),
                Some(b"new".to_vec()),
                None,
                Some(b"new".to_vec()),
                None
            ]
        );
        tx.commit().unwrap();
        drop(tx);
        let mut aborted = store.transact(true).unwrap();
        aborted.put(b"a", b"abort").unwrap();
        drop(aborted);
        let mut suffix = store.transact(true).unwrap();
        suffix.put(&[255, 255], b"last").unwrap();
        suffix.commit().unwrap();
        drop(suffix);
        let tx = store.transact(false).unwrap();
        assert_eq!(tx.get(b"a", false).unwrap(), Some(b"new".to_vec()));
        assert_eq!(
            tx.total_scan()
                .collect::<miette::Result<Vec<_>>>()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(tx.range_count(b"a", b"z").unwrap(), 2);
        assert!(tx.range_scan(b"z", b"a").next().is_none());
    }
}

#[test]
#[ignore = "requires a disposable TiKV cluster"]
fn namespace_ownership_release_reopen_and_isolation() {
    let options = config(true);
    let store = TiKvStorage::connect(options.clone()).unwrap();
    assert!(TiKvStorage::connect(options.clone()).is_err());
    let mut tx = store.transact(true).unwrap();
    tx.put(b"same", b"first").unwrap();
    tx.commit().unwrap();
    drop(tx);
    let other = TiKvStorage::connect(config(true)).unwrap();
    assert_eq!(
        other.transact(false).unwrap().get(b"same", false).unwrap(),
        None
    );
    drop(store);
    let reopened = TiKvStorage::connect(options).unwrap();
    assert_eq!(
        reopened
            .transact(false)
            .unwrap()
            .get(b"same", false)
            .unwrap(),
        Some(b"first".to_vec())
    );
}

#[test]
#[ignore = "requires a disposable TiKV cluster"]
fn recorded_history_and_fts_match_memory() {
    for optimistic in [true, false] {
        let tikv = DbInstance::TiKv(new_cozo_tikv_with_config(config(optimistic)).unwrap());
        let mem = DbInstance::new("mem", "", "").unwrap();
        for db in [&mem, &tikv] {
            run(
                db,
                ":create docs {id:String,validity:Validity=>body:String}",
            );
            run(db,"?[id,validity,body]<-[['a',[1000,true],'old alpha'],['a',[2000,true],'new alpha'],['b',[1000,true],'other beta'],['b',[2000,false],'other beta']] :put docs{id,validity=>body}");
            run(
                db,
                "::fts create docs:idx{extractor:body,tokenizer:Simple,filters:[Lowercase]}",
            );
        }
        for query in [
            "?[id,body]:=*docs{id,body @ 'NOW'}",
            "?[id,body]:=*docs{id,body @ 1500}",
            "?[id,body]:=*docs{id,body}",
            "?[id,s]:=~docs:idx{id|query:'alpha',k:20,bind_score:s}",
        ] {
            assert_eq!(
                run(&mem, query).into_json(),
                run(&tikv, query).into_json(),
                "{query}"
            );
        }
        assert_eq!(
            run(&tikv, "?[body]:=*docs{id:'a',body @ 1500}").into_json()["rows"],
            json!([["old alpha"]])
        );
        assert_eq!(
            run(&tikv, "?[id]:=*docs{id @ 'NOW'}").into_json()["rows"],
            json!([["a"]])
        );
    }
}

#[test]
#[ignore = "requires a disposable TiKV cluster"]
fn oversized_write_poisoning_and_adaptive_scan() {
    let store = TiKvStorage::connect(config(true)).unwrap();
    let mut tx = store.transact(true).unwrap();
    tx.put(b"a", b"must rollback").unwrap();
    assert!(tx.put(&vec![b'x'; 8000], b"large key").is_err());
    assert!(tx.commit().is_err());
    drop(tx);
    assert_eq!(
        store.transact(false).unwrap().get(b"a", false).unwrap(),
        None
    );
    let mut tx = store.transact(true).unwrap();
    for n in 0..8 {
        tx.put(&[b'v', n], &vec![n; 256 * 1024]).unwrap();
    }
    tx.commit().unwrap();
    drop(tx);
    drop(store);
    let mut options = config(true);
    options.max_value_bytes = 256 * 1024;
    options.max_decoding_bytes = 400 * 1024;
    let store = TiKvStorage::connect(options).unwrap();
    let mut tx = store.transact(true).unwrap();
    for n in 0..8 {
        tx.put(&[b'v', n], &vec![n; 256 * 1024]).unwrap();
    }
    tx.commit().unwrap();
    drop(tx);
    let tx = store.transact(false).unwrap();
    assert_eq!(tx.range_count(b"v", b"w").unwrap(), 8);
    assert_eq!(
        tx.multi_get(&(0..8).map(|n| vec![b'v', n]).collect::<Vec<_>>(), false)
            .unwrap()
            .len(),
        8
    );
}

#[test]
fn async_context_and_invalid_settings_return_errors() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let options = TiKvConfig {
        end_points: vec!["127.0.0.1:1".into()],
        ..Default::default()
    };
    assert!(rt
        .block_on(async { TiKvStorage::connect(options) })
        .is_err());
    assert!(TiKvStorage::connect(TiKvConfig::default()).is_err());
    assert!(serde_json::from_str::<TiKvConfig>(r#"{"end_points":[],"unknown":1}"#).is_err());
}

#[test]
#[ignore = "requires a disposable TiKV cluster"]
fn scan_errors_are_not_counts_and_iterator_is_fused() {
    let store = TiKvStorage::connect(config(true)).unwrap();
    let mut tx = store.transact(false).unwrap();
    tx.commit().unwrap();
    assert!(tx.range_count(b"a", b"z").is_err());
    let mut scan = tx.range_scan(b"a", b"z");
    assert!(scan.next().unwrap().is_err());
    assert!(scan.next().is_none());
    assert!(scan.next().is_none());
}

#[cfg(feature = "storage-sqlite")]
#[test]
#[ignore = "requires a disposable TiKV cluster"]
fn full_backup_restores_history_and_fts_to_new_namespace() {
    let source = DbInstance::TiKv(new_cozo_tikv_with_config(config(true)).unwrap());
    run(
        &source,
        ":create docs{id:String,validity:Validity=>body:String}",
    );
    run(&source,"?[id,validity,body]<-[['a',[1000,true],'alpha old'],['a',[2000,true],'beta new']] :put docs{id,validity=>body}");
    run(
        &source,
        "::fts create docs:idx{extractor:body,tokenizer:Simple,filters:[Lowercase]}",
    );
    let directory = tempfile::tempdir().unwrap();
    let backup = directory.path().join("full.sqlite");
    source.backup_db(&backup).unwrap();
    let restored_options = config(true);
    let restored = DbInstance::TiKv(new_cozo_tikv_with_config(restored_options.clone()).unwrap());
    restored.restore_backup(&backup).unwrap();
    for query in [
        "?[body]:=*docs{id:'a',body @ 1500}",
        "?[body]:=*docs{id:'a',body @ 'NOW'}",
        "?[id,s]:=~docs:idx{id|query:'beta',k:20,bind_score:s}",
    ] {
        assert_eq!(
            run(&source, query).into_json(),
            run(&restored, query).into_json()
        );
    }
    drop(restored);
    let reopened = DbInstance::TiKv(new_cozo_tikv_with_config(restored_options).unwrap());
    assert_eq!(
        run(&reopened, "?[body]:=*docs{id:'a',body @ 1500}").into_json()["rows"],
        json!([["alpha old"]])
    );
}

#[test]
#[ignore = "requires a disposable TiKV cluster"]
fn waiting_writer_starts_its_snapshot_after_previous_commit() {
    for optimistic in [true, false] {
        let store = TiKvStorage::connect(config(optimistic)).unwrap();
        let mut seed = store.transact(true).unwrap();
        seed.put(b"key", b"old").unwrap();
        seed.commit().unwrap();
        drop(seed);
        let mut first = store.transact(true).unwrap();
        let other = store.clone();
        let (started, starting) = std::sync::mpsc::channel();
        let (read, observed) = std::sync::mpsc::channel();
        let task = std::thread::spawn(move || {
            started.send(()).unwrap();
            let mut second = other.transact(true).unwrap();
            let value = second.get(b"key", false).unwrap();
            second.commit().unwrap();
            read.send(value).unwrap();
        });
        starting.recv().unwrap();
        assert!(matches!(
            observed.recv_timeout(std::time::Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        first.put(b"key", b"new").unwrap();
        first.commit().unwrap();
        drop(first);
        assert_eq!(
            observed
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            Some(b"new".to_vec())
        );
        task.join().unwrap();
    }
}
