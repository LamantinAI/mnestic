# Transactional TiKV storage

Enable `storage-tikv` to use the Rust transactional client. TiKV requires PD
even for a single-store fixture: PD supplies timestamps and region routing.
Client traffic carrying stored values goes directly to TiKV.

```rust,no_run
use cozo::{new_cozo_tikv_with_config, TiKvConfig};

let db = new_cozo_tikv_with_config(TiKvConfig {
    end_points: vec!["127.0.0.1:2379".into()],
    namespace: Some("application-memory".into()),
    ..Default::default()
})?;
# Ok::<(), cozo::Error>(())
```

`DbInstance::new("tikv", "", options_json)` accepts the same `TiKvConfig`.
The `path` argument is not a namespace. The legacy `new_cozo_tikv` constructor
retains its unprefixed layout; its caller must guarantee exclusive use of that
key range and must not mix it with namespaced databases.

## Settings

| Field | Default / meaning |
| --- | --- |
| `end_points` | Required PD endpoints. `endpoints` is an accepted alias. |
| `optimistic` | `false`, pessimistic transactions. |
| `namespace` | Optional engine-level key prefix; namespaced databases enforce exclusive runtime ownership. |
| `keyspace` | Optional existing keyspace on an APIv2-enabled cluster. |
| `tls` | Optional `{ca, cert, key}` paths, applied to both PD and TiKV. |
| `request_timeout_ms` | 3000, per RPC; SDK lock/region retries may take longer. |
| `scan_batch_size` | 100 rows, reduced automatically on decoding-size errors. |
| `max_decoding_bytes` | 16MiB. |
| `max_value_bytes` | 6MiB, leaving room under the usual 8MiB Raft entry limit. |
| `lease_ttl_ms` | 60000; must allow at least four RPC timeouts. |

Keys are checked against TiKV's 8192-byte **encoded** limit, including physical
namespace and APIv2 prefixes. A value or key that exceeds a limit is rejected;
it is never truncated. A failed mutation poisons the transaction. Batch reads
retain input ordering, duplicate keys and missing values. Pessimistic
get-for-update returns the transaction's own pending changes when present.

## Runtime and transaction boundaries

This is a synchronous storage API. Initialize and execute it on blocking
threads outside Tokio; calls made inside a Tokio runtime return an error.
An unfinished transaction is explicitly rolled back on drop. An indeterminate
primary commit response is different: `TiKvCommitUnknown` is returned, rollback
and replay are forbidden, and that runtime refuses further operations until
reopened. A lost acknowledgement is not proof that the transaction aborted.

Validity scans support current and historical selectors. Persisted-only range
deletion reads the start snapshot and preserves pending puts/deletes. Scan
errors propagate to counts; an errored iterator terminates after that error.
Whole-store scans include high-byte keys and remove the namespace prefix from
returned keys.

## Ownership and recovery

One independent `Db` runtime may own a namespace at a time; share clones of that
same `Db`, not separately initialized engine caches over a cloned storage
handle. A renewable lease controls admission, and every write transaction
validates a separate ownership token within its own transaction. Heartbeats
update a different row, so renewal does not invalidate long-running optimistic
writes. A former owner cannot commit after a new owner changes the token.
Lease loss fails closed and requires reopening to refresh process-local caches.
Active pessimistic transactions may delay takeover until their locks resolve.
Within one storage handle, writers are admitted before acquiring a TiKV
snapshot, so a writer queued behind another commit does not retain stale
start-timestamp reads. Readers may still open independent snapshots.

Namespaces and PD keyspaces organize storage; they do not enforce application
authorization. Do not bypass ownership with external KV writes. TiKV
replication does not synchronize relation, FTS or graph-projection caches in
several independent runtimes.

With `storage-sqlite`, `backup_db` captures the engine's raw-key snapshot and
`restore_backup` restores it atomically into an empty database. This includes
recorded history and FTS data; it is separate from an application's export of
current knowledge. Large restores can exceed server transaction limits and
must be qualified rather than silently split into visible partial batches.

Standalone TiKV needs an operator-owned MVCC GC policy protecting active
snapshots and backups. This adapter never advances the cluster safepoint.
Storage MVCC garbage collection is separate from explicit recorded validity
rows. HA, crash/partition recovery, capacity and production backup tooling need
deployment-specific qualification; single-store tests do not establish them.
