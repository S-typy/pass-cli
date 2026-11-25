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

use crate::commands::{OutputFormat, settings_helper};
use crate::helpers::CliPassClient as PassClient;
use anyhow::{Context, Result};
use pass::CreateVaultArgs;

#[derive(serde::Serialize)]
struct VaultCreatedOutput {
    name: String,
    vault_id: String,
    share_id: String,
}

pub async fn run(client: PassClient, name: String, output: Option<OutputFormat>) -> Result<()> {
    let output = match output {
        Some(fmt) => fmt,
        None => settings_helper::get_default_format(&client)
            .await?
            .unwrap_or(OutputFormat::Human),
    };
    let args = CreateVaultArgs::new(name.clone()).context("invalid args for create vault")?;
    let (share_id, vault_id) = client
        .create_vault(args)
        .await
        .context("error creating vault")?;

    match output {
        OutputFormat::Human => {
            println!("{share_id}");
        }
        OutputFormat::Json => {
            let payload = VaultCreatedOutput {
                name,
                vault_id: vault_id.to_string(),
                share_id: share_id.to_string(),
            };
            let json = serde_json::to_string_pretty(&payload)
                .context("error serializing created vault output")?;
            println!("{json}");
        }
    }
    Ok(())
}
