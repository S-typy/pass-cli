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

use crate::commands::item::list::ItemSummary;
use crate::commands::{OutputFormat, settings_helper};
use crate::helpers::CliPassClient as PassClient;
use anyhow::{Context, Result, anyhow};
use clap::Args;
use futures::stream::{self, StreamExt};
use pass_domain::{EventId, Item, ItemId, ItemState, ShareId, Vault};
use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Args)]
pub struct SyncArgs {
    #[arg(
        long,
        conflicts_with_all = ["vault_name", "all_vaults"],
        help = "Share ID of the vault to sync"
    )]
    pub share_id: Option<String>,

    #[arg(
        long = "vault-name",
        value_name = "VAULT_NAME",
        conflicts_with_all = ["share_id", "all_vaults"],
        help = "Name of the vault to sync"
    )]
    pub vault_name: Option<String>,

    #[arg(
        long,
        conflicts_with_all = ["share_id", "vault_name"],
        help = "Sync across all accessible vaults"
    )]
    pub all_vaults: bool,

    #[arg(
        long,
        help = "Previous sync event cursor returned by an earlier 'item sync' call"
    )]
    pub since_event: Option<String>,

    #[arg(
        long,
        help = "Include full item content in JSON output (requires --output json, not allowed with agent sessions)"
    )]
    pub show_secrets: bool,

    #[arg(long)]
    pub output: Option<OutputFormat>,

    #[arg(
        long,
        default_value_t = 50,
        help = "Maximum event pages per delta; continue with last_event_id while has_more is true"
    )]
    pub max_event_pages: usize,

    #[arg(
        long,
        default_value_t = 8,
        help = "Parallel concurrency when fetching updated items"
    )]
    pub concurrency: usize,

    #[arg(
        long,
        help = "Return mode=full_refresh_required instead of performing a full snapshot on full_refresh"
    )]
    pub no_fallback_full: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
struct SyncVaultInfo {
    share_id: ShareId,
    name: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct SyncDeletedItem {
    share_id: ShareId,
    pub(super) item_id: ItemId,
}

#[derive(serde::Serialize)]
pub(super) struct SyncResponse<T: serde::Serialize> {
    pub(super) mode: &'static str,
    pub(super) last_event_id: String,
    pub(super) full_refresh: bool,
    pub(super) has_more: bool,
    vaults: Vec<SyncVaultInfo>,
    pub(super) items: Vec<T>,
    pub(super) deleted_items: Vec<SyncDeletedItem>,
}

fn page_has_more(pending: bool, current: &EventId, next: &EventId) -> Result<bool> {
    anyhow::ensure!(!next.value().is_empty(), "Missing event cursor");
    anyhow::ensure!(
        !pending || current != next,
        "Pending events without cursor progress"
    );
    Ok(pending)
}

async fn resolve_target_vaults(client: &PassClient, args: &SyncArgs) -> Result<Vec<SyncVaultInfo>> {
    let all_vaults: Vec<Vault> = client
        .list_vaults_for_sync()
        .await
        .context("Error listing vaults")?;
    let name_by_share: HashMap<String, String> = all_vaults
        .iter()
        .map(|v| (v.share_id.to_string(), v.content.name.clone()))
        .collect();

    if args.all_vaults {
        return Ok(all_vaults
            .into_iter()
            .map(|v| SyncVaultInfo {
                share_id: v.share_id,
                name: v.content.name,
            })
            .collect());
    }

    if let Some(ref sid) = args.share_id {
        let name = name_by_share
            .get(sid)
            .cloned()
            .context("Requested share is not an accessible vault")?;
        return Ok(vec![SyncVaultInfo {
            share_id: ShareId::new(sid.clone()),
            name,
        }]);
    }

    if let Some(ref vault_name) = args.vault_name {
        let mut matches = all_vaults
            .into_iter()
            .filter(|v| v.content.name == *vault_name);
        let vault = matches
            .next()
            .ok_or_else(|| anyhow!("Could not find vault {vault_name}"))?;
        anyhow::ensure!(
            matches.next().is_none(),
            "Ambiguous vault name; select the vault by share ID"
        );
        return Ok(vec![SyncVaultInfo {
            share_id: vault.share_id,
            name: vault.content.name,
        }]);
    }

    if let Some(default_share_id) = settings_helper::get_default_share_id(client).await? {
        let key = default_share_id.to_string();
        let name = name_by_share
            .get(&key)
            .cloned()
            .context("Default vault is unavailable")?;
        return Ok(vec![SyncVaultInfo {
            share_id: default_share_id,
            name,
        }]);
    }

    Err(anyhow!(
        "Please provide --all-vaults, --share-id, --vault-name, or set a default vault with 'pass-cli settings set default-vault'"
    ))
}

pub async fn run(args: SyncArgs, client: PassClient) -> Result<()> {
    let output = match &args.output {
        Some(fmt) => fmt.clone(),
        None => OutputFormat::Json,
    };

    if args.show_secrets && client.is_agent_session() {
        return Err(anyhow!(
            "--show-secrets is not allowed with an agent session"
        ));
    }
    if args.show_secrets && !matches!(output, OutputFormat::Json) {
        return Err(anyhow!("--show-secrets requires --output json"));
    }

    let response = read_changes(&args, &client).await?;
    emit_sync_output(&output, args.show_secrets, response)
}

pub(super) async fn read_changes(
    args: &SyncArgs,
    client: &PassClient,
) -> Result<SyncResponse<Item>> {
    let since_event = args
        .since_event
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| EventId::new(s.to_string()));

    let max_event_pages = args.max_event_pages.clamp(1, 500);
    let concurrency = args.concurrency.clamp(1, 16);

    if let Some(mut cursor) = since_event {
        let target_vaults = resolve_target_vaults(client, args).await?;
        let target_share_set: HashSet<String> = target_vaults
            .iter()
            .map(|v| v.share_id.to_string())
            .collect();

        let mut updated_keys: BTreeSet<(String, String)> = BTreeSet::new();
        let mut deleted_keys: BTreeSet<(String, String)> = BTreeSet::new();
        let mut needs_full_refresh = false;

        let mut has_more = false;
        for _ in 0..max_event_pages {
            let events = client
                .get_user_events(&cursor)
                .await
                .context("Error fetching Proton Pass sync events")?;

            if events.full_refresh || events.refresh_user {
                needs_full_refresh = true;
                break;
            }
            let mut share_changes = events
                .shares_created
                .iter()
                .chain(&events.shares_updated)
                .chain(&events.shares_deleted);
            if share_changes.any(|s| {
                args.all_vaults
                    || args.vault_name.is_some()
                    || target_share_set.contains(s.share_id.value())
            }) {
                needs_full_refresh = true;
                break;
            }

            // Events are grouped by type within a page. If both groups contain the
            // same item, fetch its current revision to settle its final state.
            for ev in events.items_deleted {
                let sid = ev.share_id.to_string();
                if !target_share_set.contains(&sid) {
                    continue;
                }
                let pair = (sid, ev.item_id.to_string());
                updated_keys.remove(&pair);
                deleted_keys.insert(pair);
            }

            for ev in events
                .items_updated
                .into_iter()
                .chain(events.alias_note_changed)
            {
                let sid = ev.share_id.to_string();
                if !target_share_set.contains(&sid) {
                    continue;
                }
                let pair = (sid, ev.item_id.to_string());
                deleted_keys.remove(&pair);
                updated_keys.insert(pair);
            }
            let next_cursor = events.last_event_id;
            has_more = page_has_more(events.events_pending, &cursor, &next_cursor)?;
            cursor = next_cursor;
            if !has_more {
                break;
            }
        }

        if !needs_full_refresh {
            let pairs: Vec<(String, String)> = updated_keys.into_iter().collect();
            let fetched_results = stream::iter(pairs)
                .map(|(sid, iid)| {
                    let c = client;
                    async move {
                        let share = ShareId::new(sid.clone());
                        let item = ItemId::new(iid.clone());
                        c.get_item_for_sync(&share, &item)
                            .await
                            .map(|value| value.ok_or((share, item)))
                    }
                })
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await;

            let mut items: Vec<Item> = Vec::with_capacity(fetched_results.len());
            for res in fetched_results {
                match res? {
                    Ok(item) if item.state == ItemState::Active => items.push(item),
                    Ok(item) => {
                        deleted_keys.insert((item.share_id.to_string(), item.id.to_string()));
                    }
                    Err((share, item)) => {
                        deleted_keys.insert((share.to_string(), item.to_string()));
                    }
                }
            }
            items.sort_by(|a, b| {
                a.share_id
                    .value()
                    .cmp(b.share_id.value())
                    .then_with(|| a.id.value().cmp(b.id.value()))
            });

            let deleted_items: Vec<SyncDeletedItem> = deleted_keys
                .into_iter()
                .map(|(sid, iid)| SyncDeletedItem {
                    share_id: ShareId::new(sid),
                    item_id: ItemId::new(iid),
                })
                .collect();

            return Ok(SyncResponse {
                mode: "incremental",
                last_event_id: cursor.to_string(),
                full_refresh: false,
                has_more,
                vaults: target_vaults,
                items,
                deleted_items,
            });
        }

        if args.no_fallback_full {
            return Ok(SyncResponse {
                mode: "full_refresh_required",
                last_event_id: String::new(),
                full_refresh: false,
                has_more: false,
                vaults: target_vaults,
                items: vec![],
                deleted_items: vec![],
            });
        }
    }

    // Full snapshot path: record the event cursor BEFORE listing items so concurrent
    // writes during the vault walk are never missed on the subsequent incremental run.
    let snapshot_cursor = client
        .get_last_user_event_id()
        .await
        .context("Error fetching baseline event ID before full sync")?;

    anyhow::ensure!(
        !snapshot_cursor.value().is_empty(),
        "Missing baseline cursor"
    );
    let target_vaults = resolve_target_vaults(client, args).await?;

    let mut all_items: Vec<Item> = Vec::new();
    for vault in &target_vaults {
        let mut vault_items = client
            .list_items_for_sync(&vault.share_id)
            .await
            .with_context(|| format!("Error listing items for share {}", vault.share_id))?;
        vault_items.retain(|item| item.state == ItemState::Active);
        all_items.extend(vault_items);
    }
    all_items.sort_by(|a, b| {
        a.share_id
            .value()
            .cmp(b.share_id.value())
            .then_with(|| a.id.value().cmp(b.id.value()))
    });

    Ok(SyncResponse {
        mode: "full",
        last_event_id: snapshot_cursor.to_string(),
        full_refresh: true,
        has_more: false,
        vaults: target_vaults,
        items: all_items,
        deleted_items: vec![],
    })
}

fn emit_sync_output(
    output: &OutputFormat,
    show_secrets: bool,
    response: SyncResponse<Item>,
) -> Result<()> {
    let SyncResponse {
        mode,
        last_event_id,
        full_refresh,
        has_more,
        vaults,
        items,
        deleted_items,
    } = response;
    match output {
        OutputFormat::Json => {
            let json = if show_secrets {
                let payload = SyncResponse {
                    mode,
                    last_event_id,
                    full_refresh,
                    has_more,
                    vaults,
                    items,
                    deleted_items,
                };
                serde_json::to_string(&payload).context("Error serializing sync output")?
            } else {
                let summaries: Vec<ItemSummary> = items.iter().map(ItemSummary::from).collect();
                let payload = SyncResponse {
                    mode,
                    last_event_id,
                    full_refresh,
                    has_more,
                    vaults,
                    items: summaries,
                    deleted_items,
                };
                serde_json::to_string(&payload).context("Error serializing sync output")?
            };
            println!("{json}");
        }
        OutputFormat::Human => {
            println!(
                "mode={mode} last_event_id={last_event_id} full_refresh={full_refresh} has_more={has_more} vaults={} items={} deleted={}",
                vaults.len(),
                items.len(),
                deleted_items.len()
            );
            for item in items {
                println!(
                    "- [{} / {}]: {} (state={:?})",
                    item.share_id, item.id, item.content.title, item.state
                );
            }
            for deleted in deleted_items {
                println!("- [deleted] {} / {}", deleted.share_id, deleted.item_id);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_limit_can_return_a_resumable_delta() {
        let a = EventId::new("a".into());
        let b = EventId::new("b".into());
        assert!(page_has_more(true, &a, &b).unwrap());
        assert!(!page_has_more(false, &a, &a).unwrap());
        assert!(page_has_more(true, &a, &a).is_err());
    }
}
