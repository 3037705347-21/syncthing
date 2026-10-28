stdiscosrv
==========

This is the global discovery server for the `syncthing` project.

Usage
-----

https://docs.syncthing.net/users/stdiscosrv.html

Device retirement
-----------------

A device can be administratively retired so that its old addresses stop
being returned by queries and can not be resurrected by late
announcements or replication messages:

```
# Retire a device (returns 204)
curl -X POST 'https://discovery.example.com/v2/retire?device=<DEVICE-ID>'

# Re-enable a retired device (returns 204; 404 if it isn't retired)
curl -X POST 'https://discovery.example.com/v2/enable?device=<DEVICE-ID>'
```

Retirement writes a durable tombstone with a strictly increasing
*deletion generation*. Plain announcements never carry a generation and
are always ignored for retired devices; replicated retire/re-enable
events only take effect when their generation is newer. Re-enabling
requires an explicit call that bumps the generation, after which the
device must announce itself again.

The tombstone is journaled (fsynced `events.log`), included in the
snapshot, and uploaded to the backup storage before the request
succeeds, so a crash or interrupted write at any point is recoverable:
the snapshot is written atomically (`records.db.tmp` ->
`records.db.prev` -> `records.db`, directory fsynced), the previous
snapshot is retained and used if the current one is corrupt, and failed
backup uploads leave the old backup object intact and are retried (the
retire call can also simply be repeated, it is idempotent while the
device stays retired). These endpoints have no authentication of their
own and must be restricted at the network/proxy level.
