# Incremental synchronization

Run `pass-cli item sync --all-vaults` for the initial snapshot. Apply the result
and save its `last_event_id` in the same transaction. On the next run, pass that
value as `--since-event` with the same vault scope and account.
The JSON output defaults to item summaries.

* In `full` mode replace the local records within the requested scope.
* In `incremental` mode upsert the returned items by share ID and item ID, and
  remove only IDs listed in `deleted_items`. Empty arrays mean no changes.
  When `has_more` is true, continue with the returned cursor. Reapplying a page
  should produce the same local state.
* Proton can require a new snapshot. With `--no-fallback-full` the CLI returns
  `mode=full_refresh_required` and no usable cursor. Keep the previous local
  data and schedule a full snapshot. On CLI errors, retain the last committed cursor.

## Idempotent login creation

Use a stable, non-nil UUID in the optional JSON template field `item_uuid` for
`item create login --from-template` or `item create login-batch --from-template`.
Use the same UUID across retries, including a retry of an individual entry from
a batch. Every entry in a keyed batch must provide a UUID. For example:

```json
{"title":"Example login","username":"demo","item_uuid":"a96b1223-ea62-4463-8c8f-7ee6be92539b"}
```

The key identifies one active item in one vault. Creation returns the existing
item ID on a match. Use the update command to change its contents. Identical
UUIDs within a batch retain their positions in the result. Conflicting input
for the same UUID within one batch or multiple existing matches cause an error.
Templates without a UUID keep the usual create behavior. Legacy items created
with random UUIDs do not match newly assigned UUIDs.

The first keyed write builds an index for that vault. Subsequent writes update
it from the event cursor. The session directory holds a SQLite index of UUIDs,
item IDs, the cursor and pending write markers. A marker is committed before a
write. If the response is lost, retry with the same UUID. Once Proton exposes
the created item, the CLI can return its ID. While the outcome is unknown,
the CLI rejects a second create. A crash before the request is sent can also
leave a pending marker requiring investigation.

Concurrent writers should use one writer service or the same local CLI session
directory. Independent hosts do not share the pre-write reservation.
