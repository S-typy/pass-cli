/*
 *  Copyright (c) 2026 Proton AG
 *  This file is part of Proton AG and Proton Pass.
 *
 *  Proton Pass is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Proton Pass is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Proton Pass.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

use crate::commands::item::agent_monitor::send_reason_if_agent;
use crate::commands::item::sync::{SyncArgs, read_changes};
use crate::helpers::CliPassClient;
use anyhow::{Context, Result, ensure};
use pass_db::rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use pass_domain::{EventAction, FolderId, ItemData, ItemId, ShareId};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

// One database per share. Store only opaque identifiers and the event cursor.
fn initialize(conn: &Connection) -> Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS entries (
        item_id TEXT PRIMARY KEY, uuid TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS by_uuid ON entries(uuid);
    CREATE TABLE IF NOT EXISTS pending(uuid TEXT PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS checkpoint(singleton INTEGER PRIMARY KEY CHECK(singleton=1), cursor TEXT NOT NULL);")?;
    Ok(())
}

fn existing(conn: &Connection, uuid: &str) -> Result<Option<ItemId>> {
    let mut stmt = conn.prepare("SELECT item_id FROM entries WHERE uuid=?1 LIMIT 2")?;
    let ids = stmt
        .query_map([uuid], |r| r.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    ensure!(
        ids.len() < 2,
        "Multiple active items have the supplied item_uuid; resolve the conflict before retrying"
    );
    Ok(ids.into_iter().next().map(ItemId::new))
}

async fn refresh(conn: &mut Connection, client: &CliPassClient, share: &ShareId) -> Result<()> {
    loop {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let since_event = tx
            .query_row("SELECT cursor FROM checkpoint WHERE singleton=1", [], |r| {
                r.get(0)
            })
            .optional()?;
        let args = SyncArgs {
            share_id: Some(share.to_string()),
            vault_name: None,
            all_vaults: false,
            since_event,
            show_secrets: false,
            output: None,
            max_event_pages: 50,
            concurrency: 8,
            no_fallback_full: false,
        };
        let delta = read_changes(&args, client).await?;
        if delta.full_refresh {
            tx.execute("DELETE FROM entries", [])?;
        }
        for removed in delta.deleted_items {
            tx.execute(
                "DELETE FROM entries WHERE item_id=?1",
                [removed.item_id.value()],
            )?;
        }
        for item in delta.items {
            // Legacy items can have an empty or non-UUID local identifier.
            if let Ok(uuid) = uuid::Uuid::parse_str(&item.content.item_uuid) {
                tx.execute(
                    "INSERT OR REPLACE INTO entries(item_id,uuid) VALUES(?1,?2)",
                    [item.id.value(), &uuid.to_string()],
                )?;
            } else {
                tx.execute("DELETE FROM entries WHERE item_id=?1", [item.id.value()])?;
            }
        }
        tx.execute(
            "INSERT OR REPLACE INTO checkpoint(singleton,cursor) VALUES(1,?1)",
            [delta.last_event_id],
        )?;
        tx.commit()?;
        if !delta.has_more {
            return Ok(());
        }
    }
}

// Commit these markers BEFORE sending a create. After an ambiguous response the
// next run first consumes remote events. Never issue a second blind POST.
fn reserve(
    conn: &mut Connection,
    items: &[ItemData],
    resolved: &mut HashMap<String, ItemId>,
) -> Result<Vec<ItemData>> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut missing = Vec::new();
    for item in items {
        let uuid = &item.item_uuid;
        if let Some(id) = existing(&tx, uuid)? {
            tx.execute("DELETE FROM pending WHERE uuid=?1", [uuid])?;
            resolved.insert(uuid.clone(), id);
        } else {
            ensure!(
                tx.execute("INSERT OR IGNORE INTO pending(uuid) VALUES(?1)", [uuid])? == 1,
                "A previous create with item_uuid {uuid} has an unknown outcome. Retry with the same UUID after synchronization"
            );
            missing.push(item.clone());
        }
    }
    tx.commit()?;
    Ok(missing)
}

fn unique_inputs(items: Vec<ItemData>) -> Result<(Vec<String>, Vec<ItemData>)> {
    let mut by_uuid: HashMap<String, usize> = HashMap::new();
    let mut unique = Vec::new();
    let mut order = Vec::with_capacity(items.len());
    for item in items {
        let uuid = item.item_uuid.clone();
        order.push(uuid.clone());
        if let Some(&idx) = by_uuid.get(&uuid) {
            ensure!(
                unique[idx] == item,
                "Conflicting templates for item_uuid {uuid}"
            );
        } else {
            by_uuid.insert(uuid, unique.len());
            unique.push(item);
        }
    }
    Ok((order, unique))
}

pub(super) enum WriteMode<'a> {
    Single(Option<&'a FolderId>),
    Batch,
}

pub(super) async fn create(
    client: &CliPassClient,
    share: &ShareId,
    items: Vec<ItemData>,
    mode: WriteMode<'_>,
) -> Result<Vec<ItemId>> {
    let (order, unique) = unique_inputs(items)?;
    let filename = format!(
        "item-uuids-{:x}.sqlite",
        Sha256::digest(share.value().as_bytes())
    );
    let mut conn = Connection::open(crate::utils::get_base_dir()?.join(filename))?;
    conn.busy_timeout(std::time::Duration::from_secs(10))?;
    initialize(&conn)?;
    refresh(&mut conn, client, share).await?;
    let mut resolved = HashMap::new();
    for chunk in unique.chunks(100) {
        let missing = reserve(&mut conn, chunk, &mut resolved)?;
        let reused: Vec<_> = chunk
            .iter()
            .filter_map(|item| resolved.get(&item.item_uuid).cloned())
            .collect();
        if missing.is_empty() {
            for id in &reused {
                send_reason_if_agent(client, EventAction::ItemRead, share, Some(id)).await?;
            }
            continue;
        }
        let ids = if let WriteMode::Single(folder) = mode {
            vec![
                client
                    .create_login_data(share, missing[0].clone(), folder)
                    .await?,
            ]
        } else {
            client.create_items(share, missing.clone()).await?
        };
        ensure!(ids.len() == missing.len(), "Incomplete create response");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (item, id) in missing.into_iter().zip(&ids) {
            tx.execute(
                "INSERT OR REPLACE INTO entries(item_id,uuid) VALUES(?1,?2)",
                [id.value(), &item.item_uuid],
            )?;
            tx.execute("DELETE FROM pending WHERE uuid=?1", [&item.item_uuid])?;
            resolved.insert(item.item_uuid, id.clone());
        }
        tx.commit()?;
        for id in &reused {
            send_reason_if_agent(client, EventAction::ItemRead, share, Some(id)).await?;
        }
        for id in &ids {
            send_reason_if_agent(client, EventAction::ItemCreate, share, Some(id)).await?;
        }
    }
    order
        .into_iter()
        .map(|uuid| {
            resolved
                .get(&uuid)
                .cloned()
                .context("Missing item ID after create")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::login::LoginTemplate;
    use super::super::login_batch::templates_to_item_data;
    use super::*;

    fn item() -> ItemData {
        templates_to_item_data(vec![LoginTemplate {
            title: "fixture".into(),
            item_uuid: Some("00000000-0000-4000-8000-000000000001".into()),
            ..Default::default()
        }])
        .unwrap()
        .remove(0)
    }

    #[test]
    fn repeated_templates_preserve_positions_and_disagreeing_inputs_fail() {
        let first = item();
        let (order, unique) = unique_inputs(vec![first.clone(), first.clone()]).unwrap();
        assert_eq!(order.len(), 2);
        assert_eq!(unique.len(), 1);
        let mut changed = first.clone();
        changed.title = "different".into();
        assert!(unique_inputs(vec![first, changed]).is_err());
    }

    #[test]
    fn unresolved_entry_rolls_back_other_reservations_in_same_chunk() {
        let mut db = Connection::open_in_memory().unwrap();
        initialize(&db).unwrap();
        let first = item();
        let mut second = first.clone();
        second.item_uuid = "00000000-0000-4000-8000-000000000002".into();
        let mut resolved = HashMap::new();
        reserve(&mut db, std::slice::from_ref(&second), &mut resolved).unwrap();
        assert!(reserve(&mut db, &[first.clone(), second], &mut resolved).is_err());
        assert_eq!(reserve(&mut db, &[first], &mut resolved).unwrap().len(), 1);
    }

    #[test]
    fn interrupted_create_survives_restart_and_can_be_reconciled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let input = [item()];
        let uuid = &input[0].item_uuid;
        let mut resolved = HashMap::new();
        {
            let mut conn = Connection::open(&path).unwrap();
            initialize(&conn).unwrap();
            assert_eq!(reserve(&mut conn, &input, &mut resolved).unwrap().len(), 1);
        }
        let mut conn = Connection::open(path).unwrap();
        assert!(reserve(&mut conn, &input, &mut resolved).is_err());
        conn.execute("INSERT INTO entries VALUES('remote',?1)", [uuid])
            .unwrap();
        assert!(
            reserve(&mut conn, &input, &mut resolved)
                .unwrap()
                .is_empty()
        );
        assert_eq!(resolved[uuid].value(), "remote");
        conn.execute("INSERT INTO entries VALUES('second',?1)", [uuid])
            .unwrap();
        assert!(reserve(&mut conn, &input, &mut resolved).is_err());
    }
}
