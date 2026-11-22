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

use super::login::LoginTemplate;
use crate::commands::item::agent_monitor::{ensure_reason_if_agent, send_reason_if_agent};
use crate::commands::{OutputFormat, item::common::ShareQuery, settings_helper};
use crate::helpers::CliPassClient as PassClient;
use anyhow::{Context, Result, anyhow};
use clap::Args;
use pass_domain::{EventAction, ItemContent, ItemData, ItemId, LoginItem, ShareId};
use std::io::{self, Read};

#[derive(Args, Default, PartialEq, Eq)]
pub struct LoginBatchArgs {
    /// Create multiple login items from a JSON array template file or '-' for stdin
    #[arg(
        long,
        help = "JSON array file or '-' for stdin; supply item_uuid on every entry for idempotent creation"
    )]
    from_template: String,

    /// Share ID of the vault to create the login items in
    #[arg(long, help = "Share ID of the vault to create the login items in")]
    share_id: Option<String>,

    /// Name of the vault to create the login items in
    #[arg(long, help = "Name of the vault to create the login items in")]
    vault_name: Option<String>,

    /// Output format
    #[arg(long)]
    output: Option<OutputFormat>,
}

#[derive(serde::Serialize)]
struct BatchCreateOutput {
    share_id: ShareId,
    item_ids: Vec<ItemId>,
}

pub(crate) fn templates_to_item_data(templates: Vec<LoginTemplate>) -> Result<Vec<ItemData>> {
    let mut items = Vec::with_capacity(templates.len());
    for (idx, tmpl) in templates.into_iter().enumerate() {
        if tmpl.title.trim().is_empty() {
            return Err(anyhow!("Login template at index {idx} has an empty title"));
        }
        items.push(ItemData {
            title: tmpl.title,
            note: tmpl.note.unwrap_or_default(),
            item_uuid: match tmpl.item_uuid {
                Some(value) => {
                    let id = uuid::Uuid::parse_str(&value)
                        .with_context(|| format!("Invalid item_uuid at index {idx}"))?;
                    anyhow::ensure!(!id.is_nil(), "Empty item_uuid at index {idx}");
                    id.to_string()
                }
                None => ItemData::generate_uuid(),
            },
            content: ItemContent::Login(LoginItem {
                email: tmpl.email.unwrap_or_default(),
                username: tmpl.username.unwrap_or_default(),
                password: tmpl.password.unwrap_or_default(),
                urls: tmpl.urls,
                totp_uri: tmpl.totp_uri.unwrap_or_default(),
                passkeys: tmpl.passkeys,
            }),
            extra_fields: vec![],
            platform_specific: None,
        });
    }
    Ok(items)
}

pub async fn run(mut args: LoginBatchArgs, client: PassClient) -> Result<()> {
    if args.share_id.is_none() && args.vault_name.is_none() {
        args.share_id = settings_helper::get_default_share_id(&client)
            .await?
            .map(|id| id.to_string());
    }

    let output = settings_helper::get_format(args.output, &client).await?;
    let share_query = ShareQuery::new(args.share_id, args.vault_name)?;

    let contents = if args.from_template == "-" {
        let mut stdin = io::stdin();
        let mut buf = String::new();
        stdin
            .read_to_string(&mut buf)
            .context("Error reading batch login templates from stdin")?;
        buf
    } else {
        std::fs::read_to_string(&args.from_template).with_context(|| {
            format!(
                "Error reading batch login template file: {}",
                args.from_template
            )
        })?
    };

    let templates = serde_json::from_str::<Vec<LoginTemplate>>(&contents)
        .context("Error parsing JSON array of login templates")?;

    ensure_reason_if_agent(&client)?;
    let share_id = share_query.share_id(&client).await?;

    let keyed = templates.iter().filter(|t| t.item_uuid.is_some()).count();
    anyhow::ensure!(
        keyed == 0 || keyed == templates.len(),
        "Provide item_uuid for every entry in an idempotent batch"
    );
    let items = templates_to_item_data(templates)?;
    let item_ids = if keyed > 0 {
        super::unique_login::create(
            &client,
            &share_id,
            items,
            super::unique_login::WriteMode::Batch,
        )
        .await?
    } else {
        let ids = client
            .create_items(&share_id, items)
            .await
            .context("Error creating login items in batch")?;
        for id in &ids {
            send_reason_if_agent(&client, EventAction::ItemCreate, &share_id, Some(id)).await?;
        }
        ids
    };

    match output {
        OutputFormat::Json => {
            let payload = BatchCreateOutput { share_id, item_ids };
            let json = serde_json::to_string_pretty(&payload)
                .context("Error serializing batch create output")?;
            println!("{json}");
        }
        OutputFormat::Human => {
            for id in item_ids {
                println!("{id}");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_to_item_data_preserves_notes_and_rejects_empty_title() {
        let ok = templates_to_item_data(vec![LoginTemplate {
            item_uuid: None,
            title: "Sony".to_string(),
            note: Some("migrated note".to_string()),
            email: Some("a@example.com".to_string()),
            username: Some("a".to_string()),
            password: Some("pw".to_string()),
            totp_uri: Some("otpauth://totp/Sony?secret=JBSWY3DPEHPK3PXP".to_string()),
            urls: vec!["https://my.account.sony.com/".to_string()],
            passkeys: vec![],
        }])
        .expect("valid template");
        assert_eq!(ok.len(), 1);
        assert_eq!(ok[0].note, "migrated note");

        let err = templates_to_item_data(vec![LoginTemplate {
            title: "   ".to_string(),
            ..Default::default()
        }])
        .expect_err("empty title should error");
        assert!(err.to_string().contains("empty title"));
    }
}
