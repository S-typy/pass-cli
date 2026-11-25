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

use crate::commands::item::agent_monitor::{ensure_reason_if_agent, send_reason_if_agent};
use crate::commands::item::common::ShareQuery;
use crate::commands::{OutputFormat, settings_helper};
use crate::helpers::CliPassClient as PassClient;
use anyhow::{Context, Result, anyhow};
use clap::Args;
use pass::MovedItemRef;
use pass_domain::{EventAction, ItemId};
use std::collections::HashSet;
use std::io::{self, Read};

#[derive(Args, Default, PartialEq, Eq)]
pub struct MoveBatchArgs {
    /// Share ID of the source vault
    #[arg(long, help = "Share ID of the source vault")]
    from_share_id: Option<String>,

    /// Name of the source vault
    #[arg(long, help = "Name of the source vault")]
    from_vault_name: Option<String>,

    /// Share ID of the destination vault
    #[arg(long, help = "Share ID of the destination vault")]
    to_share_id: Option<String>,

    /// Name of the destination vault
    #[arg(long, help = "Name of the destination vault")]
    to_vault_name: Option<String>,

    /// JSON array file of item IDs to move, or '-' for stdin
    #[arg(
        long,
        default_value = "-",
        help = "JSON array file of item IDs to move, or '-' for stdin"
    )]
    from_file: String,

    /// Maximum items per API move request (1..=100)
    #[arg(long, default_value_t = 50)]
    batch_size: usize,

    /// Concurrent item key fetch requests (1..=16)
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Output format
    #[arg(long)]
    output: Option<OutputFormat>,
}

#[derive(serde::Serialize)]
struct MoveBatchOutput {
    from_share_id: String,
    to_share_id: String,
    moved: Vec<MovedItemRef>,
}

pub(crate) fn parse_item_ids_json(raw: &str) -> Result<Vec<ItemId>> {
    let parsed =
        serde_json::from_str::<Vec<String>>(raw).context("Error parsing JSON array of item IDs")?;
    let mut seen = HashSet::with_capacity(parsed.len());
    let mut ids = Vec::with_capacity(parsed.len());
    for entry in parsed {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("Item ID list contains an empty element"));
        }
        if seen.insert(trimmed.to_string()) {
            ids.push(ItemId::new(trimmed.to_string()));
        }
    }
    if ids.is_empty() {
        return Err(anyhow!("Item ID list is empty"));
    }
    Ok(ids)
}

pub async fn run(mut args: MoveBatchArgs, client: PassClient) -> Result<()> {
    if args.from_share_id.is_none() && args.from_vault_name.is_none() {
        args.from_share_id = settings_helper::get_default_share_id(&client)
            .await?
            .map(|id| id.to_string());
    }

    let output = settings_helper::get_format(args.output, &client).await?;
    let from_share_query = ShareQuery::new_with_arg_name(
        args.from_share_id,
        args.from_vault_name,
        "--from-share-id",
        "--from-vault-name",
    )?;
    let to_share_query = ShareQuery::new_with_arg_name(
        args.to_share_id,
        args.to_vault_name,
        "--to-share-id",
        "--to-vault-name",
    )?;

    let contents = if args.from_file == "-" {
        let mut buf = String::new();
        io::stdin()
            .read_to_string(&mut buf)
            .context("Error reading item IDs from stdin")?;
        buf
    } else {
        std::fs::read_to_string(&args.from_file)
            .with_context(|| format!("Error reading item IDs file: {}", args.from_file))?
    };

    let item_ids = parse_item_ids_json(&contents)?;

    ensure_reason_if_agent(&client)?;
    let from_share_id = from_share_query.share_id(&client).await?;
    let to_share_id = to_share_query.share_id(&client).await?;

    let moved = client
        .move_items_batch(
            &from_share_id,
            &item_ids,
            &to_share_id,
            args.batch_size,
            args.concurrency,
        )
        .await
        .context("Error moving items in batch")?;

    for m in &moved {
        send_reason_if_agent(
            &client,
            EventAction::ItemSoftDelete,
            &from_share_id,
            Some(&m.old_item_id),
        )
        .await?;
        send_reason_if_agent(
            &client,
            EventAction::ItemCreate,
            &to_share_id,
            Some(&m.new_item_id),
        )
        .await?;
    }

    match output {
        OutputFormat::Json => {
            let payload = MoveBatchOutput {
                from_share_id: from_share_id.to_string(),
                to_share_id: to_share_id.to_string(),
                moved,
            };
            let json = serde_json::to_string_pretty(&payload)
                .context("Error serializing batch move output")?;
            println!("{json}");
        }
        OutputFormat::Human => {
            for m in moved {
                println!("{} -> {}", m.old_item_id, m.new_item_id);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_item_ids_json_deduplicates_preserving_order() {
        let ids = parse_item_ids_json(r#"[" id-1 ", "id-2", "id-1", "id-3"]"#).unwrap();
        let strings: Vec<_> = ids.into_iter().map(|id| id.to_string()).collect();
        assert_eq!(strings, vec!["id-1", "id-2", "id-3"]);
    }

    #[test]
    fn parse_item_ids_json_rejects_empty_and_blank_entries() {
        assert!(parse_item_ids_json("[]").is_err());
        assert!(parse_item_ids_json(r#"["id-1", "   "]"#).is_err());
        assert!(parse_item_ids_json("not-json").is_err());
    }
}
