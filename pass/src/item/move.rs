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

use crate::item::list::ItemRevision;
use crate::permission::PermissionAction;
use crate::utils::b64_encode;
use crate::{PassClient, PassClientContext};
use anyhow::{Context, Result, anyhow};
use futures::stream::{self, StreamExt};
use muon::PUT;
use pass_domain::{ItemId, ShareId, crypto};

pub const MAX_ITEMS_PER_MOVE_BATCH: usize = 100;
pub const DEFAULT_MOVE_BATCH_SIZE: usize = 50;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct MovedItemRef {
    pub old_item_id: ItemId,
    pub new_item_id: ItemId,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct MoveItemRequest {
    #[serde(rename = "ShareID")]
    pub share_id: String,
    #[serde(rename = "Items")]
    pub items: Vec<MoveItemBody>,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct MoveItemBody {
    #[serde(rename = "ItemID")]
    pub item_id: String,
    #[serde(rename = "ItemKeys")]
    pub item_keys: Vec<MoveItemKey>,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct MoveItemKey {
    #[serde(rename = "KeyRotation")]
    pub key_rotation: u8,
    #[serde(rename = "Key")]
    pub key: String,
}

#[derive(Debug, serde::Deserialize)]
struct MoveItemResponse {
    #[serde(rename = "Items")]
    items: Vec<ItemRevision>,
}

impl<C: PassClientContext> PassClient<C> {
    pub async fn move_item(
        &self,
        from_share_id: &ShareId,
        item_id: &ItemId,
        to_share_id: &ShareId,
    ) -> Result<ItemId> {
        self.action_guard(PermissionAction::DeleteItem {
            share_id: from_share_id.clone(),
            item_id: item_id.clone(),
        })
        .await?;
        self.action_guard(PermissionAction::CreateItem {
            share_id: to_share_id.clone(),
        })
        .await?;

        let body = self
            .create_move_item_request(from_share_id, item_id, to_share_id)
            .await
            .context("Error creating move item request body")?;
        let req = PUT!("/pass/v1/share/{from_share_id}/item/share")
            .body_json(body)
            .context("Error creating item move request")?;
        let res = self
            .send(req)
            .await
            .context("Error sending item move request")?;
        let response: MoveItemResponse = assert_response!(res);

        let new_item_id = response
            .items
            .first()
            .context("Error getting new item id")?
            .item_id
            .to_string();

        self.clear_items_cache(from_share_id).await;
        self.clear_items_cache(to_share_id).await;

        Ok(ItemId::new(new_item_id))
    }

    async fn create_move_item_request(
        &self,
        from_share_id: &ShareId,
        item_id: &ItemId,
        to_share_id: &ShareId,
    ) -> Result<MoveItemRequest> {
        // Get latest key rotation from API to ensure we use the most recent
        let destination_share_keys = self
            .get_share_keys(to_share_id)
            .await
            .context("Error getting destination share keys")?;
        let destination_share_key = destination_share_keys
            .latest()
            .context("Error getting destination share key")?;

        let opened_destination_share_key = self
            .get_opened_share_key_by_rotation(to_share_id, destination_share_key.key_rotation)
            .await
            .context("Error opening destination share key")?;

        let source_item_keys = self
            .get_item_keys(from_share_id, item_id)
            .await
            .context("Error getting item keys")?;
        let opened_source_item_keys = self
            .open_item_keys(from_share_id, source_item_keys)
            .await
            .context("Error opening source item keys")?;

        let migrated_item_keys = {
            let mut migrated_item_keys = Vec::with_capacity(opened_source_item_keys.len());
            for item_key in opened_source_item_keys {
                let reencrypted_item_key = crypto::encrypt(
                    item_key.key.as_ref(),
                    opened_destination_share_key.as_ref(),
                    crypto::EncryptionTag::ItemKey,
                )
                .map_err(|e| anyhow::anyhow!("Error encrypting item key: {:?}", e))?;

                migrated_item_keys.push(MoveItemKey {
                    key_rotation: item_key.key_rotation,
                    key: b64_encode(&reencrypted_item_key),
                });
            }
            migrated_item_keys
        };

        let body = MoveItemRequest {
            share_id: to_share_id.to_string(),
            items: vec![MoveItemBody {
                item_id: item_id.to_string(),
                item_keys: migrated_item_keys,
            }],
        };

        Ok(body)
    }

    pub async fn move_items_batch(
        &self,
        from_share_id: &ShareId,
        item_ids: &[ItemId],
        to_share_id: &ShareId,
        batch_size: usize,
        concurrency: usize,
    ) -> Result<Vec<MovedItemRef>> {
        if from_share_id == to_share_id {
            return Err(anyhow!("Source and destination shares must be different"));
        }
        if item_ids.is_empty() {
            return Ok(Vec::new());
        }

        let effective_batch = if batch_size == 0 {
            DEFAULT_MOVE_BATCH_SIZE
        } else {
            batch_size.min(MAX_ITEMS_PER_MOVE_BATCH)
        };
        let effective_concurrency = if concurrency == 0 {
            8
        } else {
            concurrency.clamp(1, 16)
        };

        self.action_guard(PermissionAction::CreateItem {
            share_id: to_share_id.clone(),
        })
        .await?;

        let destination_share_keys = self
            .get_share_keys(to_share_id)
            .await
            .context("Error getting destination share keys")?;
        let destination_share_key = destination_share_keys
            .latest()
            .context("Error getting destination share key")?;
        let opened_destination_share_key = self
            .get_opened_share_key_by_rotation(to_share_id, destination_share_key.key_rotation)
            .await
            .context("Error opening destination share key")?;

        let mut moved = Vec::with_capacity(item_ids.len());
        for chunk in item_ids.chunks(effective_batch) {
            let chunk_futures = chunk.iter().cloned().map(|item_id| {
                let dest_key = &opened_destination_share_key;
                async move {
                    self.delete_item_guard(from_share_id.clone(), item_id.clone())
                        .await?;
                    let source_item_keys = self
                        .get_item_keys(from_share_id, &item_id)
                        .await
                        .context("Error getting item keys")?;
                    let opened_source_item_keys = self
                        .open_item_keys(from_share_id, source_item_keys)
                        .await
                        .context("Error opening source item keys")?;

                    let mut migrated_item_keys =
                        Vec::with_capacity(opened_source_item_keys.len());
                    for item_key in opened_source_item_keys {
                        let reencrypted_item_key = crypto::encrypt(
                            item_key.key.as_ref(),
                            dest_key.as_ref(),
                            crypto::EncryptionTag::ItemKey,
                        )
                        .map_err(|e| anyhow::anyhow!("Error encrypting item key: {:?}", e))?;
                        migrated_item_keys.push(MoveItemKey {
                            key_rotation: item_key.key_rotation,
                            key: b64_encode(&reencrypted_item_key),
                        });
                    }

                    Ok::<MoveItemBody, anyhow::Error>(MoveItemBody {
                        item_id: item_id.to_string(),
                        item_keys: migrated_item_keys,
                    })
                }
            });

            let bodies: Vec<MoveItemBody> = stream::iter(chunk_futures)
                .buffered(effective_concurrency)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

            let req = PUT!("/pass/v1/share/{from_share_id}/item/share")
                .body_json(MoveItemRequest {
                    share_id: to_share_id.to_string(),
                    items: bodies,
                })
                .context("Error creating batch item move request")?;
            let res = self
                .send(req)
                .await
                .context("Error sending batch item move request")?;
            let response: MoveItemResponse = assert_response!(res);
            if response.items.len() != chunk.len() {
                return Err(anyhow!(
                    "Batch move returned {} items for {} input items",
                    response.items.len(),
                    chunk.len()
                ));
            }
            for (old_id, rev) in chunk.iter().zip(response.items.into_iter()) {
                moved.push(MovedItemRef {
                    old_item_id: old_id.clone(),
                    new_item_id: ItemId::new(rev.item_id),
                });
            }
        }

        self.clear_items_cache(from_share_id).await;
        self.clear_items_cache(to_share_id).await;

        Ok(moved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_tools::*;
    use muon::http::Method;
    use pass_domain::{PermissionFlag, TargetType};

    fn setup_two_vault_shares(server: &ProtonAPI, share_a: &str, share_b: &str) {
        let make_share = |sid: &str| crate::share::list::ShareResponse {
            share_id: sid.to_string(),
            address_id: TEST_ADDRESS_ID.to_string(),
            vault_id: format!("VAULT_{sid}"),
            target_type: TargetType::Vault.value(),
            target_id: format!("VAULT_{sid}"),
            owner: true,
            permission: PermissionFlag::Admin.value(),
            share_role_id: "1".to_string(),
            content: None,
            content_key_rotation: None,
            content_format_version: None,
            expiration_time: None,
            create_time: 0,
            group_id: None,
        };
        let sa = make_share(share_a);
        let sb = make_share(share_b);
        let sa_one = sa.clone();
        let sb_one = sb.clone();
        server.handler_with_method(
            Method::GET,
            format!("/pass/v1/share/{share_a}"),
            move |_| success(sa_one.clone()),
        );
        server.handler_with_method(
            Method::GET,
            format!("/pass/v1/share/{share_b}"),
            move |_| success(sb_one.clone()),
        );
        server.handler_with_method(Method::GET, "/pass/v1/share", move |_| {
            success(crate::share::list::GetSharesResponse {
                shares: vec![sa.clone(), sb.clone()],
            })
        });
    }

    fn setup_item_keys_handler(server: &ProtonAPI, share_id: &str, item_id: &str) {
        let encrypted = crypto::encrypt(
            &[7u8; 32],
            &TEST_SHARE_KEY,
            crypto::EncryptionTag::ItemKey,
        )
        .unwrap();
        let encoded = crate::utils::b64_encode(encrypted);
        server.handler_with_method(
            Method::GET,
            format!("/pass/v1/share/{share_id}/item/{item_id}/key"),
            move |_| {
                success(serde_json::json!({
                    "Code": 1000,
                    "Keys": {
                        "Keys": [{ "KeyRotation": 1, "Key": encoded }],
                        "Total": 1
                    }
                }))
            },
        );
    }

    #[muon_test::test]
    async fn test_move_items_batch_chunks_and_preserves_order(server: muon_test::Server) {
        let (raw_client, api) = server.client::<()>();
        let client = make_test_pass_client_with_setup(raw_client, &api, PlanType::Plus).await;

        const FROM_SHARE: &str = "FromShareID";
        const TO_SHARE: &str = "ToShareID";
        setup_two_vault_shares(&api, FROM_SHARE, TO_SHARE);
        setup_share_keys(&api, FROM_SHARE);
        setup_share_keys(&api, TO_SHARE);
        for item_id in ["old-1", "old-2", "old-3"] {
            setup_item_keys_handler(&api, FROM_SHARE, item_id);
        }

        let put_path = format!("/pass/v1/share/{FROM_SHARE}/item/share");
        api.handler_with_method(Method::PUT, put_path.clone(), |_| {
            success(serde_json::json!({
                "Code": 1000,
                "Items": [
                    ItemRevisionBuilder::new("new-1".into()).build(),
                    ItemRevisionBuilder::new("new-2".into()).build(),
                ]
            }))
        });
        api.handler_with_method(Method::PUT, put_path.clone(), |_| {
            success(serde_json::json!({
                "Code": 1000,
                "Items": [
                    ItemRevisionBuilder::new("new-3".into()).build(),
                ]
            }))
        });

        let recorder = api.new_recorder();
        let moved = client
            .move_items_batch(
                &ShareId::new(FROM_SHARE.into()),
                &[
                    ItemId::new("old-1".into()),
                    ItemId::new("old-2".into()),
                    ItemId::new("old-3".into()),
                ],
                &ShareId::new(TO_SHARE.into()),
                2,
                4,
            )
            .await
            .expect("batch move should succeed");

        assert_eq!(
            moved,
            vec![
                MovedItemRef {
                    old_item_id: ItemId::new("old-1".into()),
                    new_item_id: ItemId::new("new-1".into()),
                },
                MovedItemRef {
                    old_item_id: ItemId::new("old-2".into()),
                    new_item_id: ItemId::new("new-2".into()),
                },
                MovedItemRef {
                    old_item_id: ItemId::new("old-3".into()),
                    new_item_id: ItemId::new("new-3".into()),
                },
            ]
        );

        let put_calls: Vec<_> = recorder
            .read()
            .into_iter()
            .filter(|r| *r.method() == "PUT" && r.uri().path() == put_path)
            .collect();
        assert_eq!(put_calls.len(), 2);
        let first_body: serde_json::Value =
            serde_json::from_slice(&put_calls[0].body().to_vec()).expect("valid json");
        assert_eq!(first_body["ShareID"], TO_SHARE);
        assert_eq!(first_body["Items"][0]["ItemID"], "old-1");
        assert_eq!(first_body["Items"][1]["ItemID"], "old-2");
        assert!(first_body["Items"][0]["ItemKeys"][0]["Key"].is_string());
    }

    #[muon_test::test]
    async fn test_move_items_batch_rejects_same_share(server: muon_test::Server) {
        let (raw_client, api) = server.client::<()>();
        let client = make_test_pass_client(raw_client, &api).await;
        let same = ShareId::new("SameShare".into());
        let err = client
            .move_items_batch(&same, &[ItemId::new("item-1".into())], &same, 50, 4)
            .await
            .expect_err("same share must fail");
        assert!(err.to_string().contains("must be different"));
    }

    #[muon_test::test]
    async fn test_move_items_batch_surfaces_proton_error_message(server: muon_test::Server) {
        let (raw_client, api) = server.client::<()>();
        let client = make_test_pass_client_with_setup(raw_client, &api, PlanType::Plus).await;

        const FROM_SHARE: &str = "FromShareErr";
        const TO_SHARE: &str = "ToShareErr";
        setup_two_vault_shares(&api, FROM_SHARE, TO_SHARE);
        setup_share_keys(&api, FROM_SHARE);
        setup_share_keys(&api, TO_SHARE);
        setup_item_keys_handler(&api, FROM_SHARE, "old-1");

        api.handler_with_method(
            Method::PUT,
            format!("/pass/v1/share/{FROM_SHARE}/item/share"),
            |_| {
                response_from_status_and_payload(
                    400,
                    &serde_json::json!({
                        "Code": 300007,
                        "Error": "Vault limit 5000",
                        "Details": {}
                    }),
                )
            },
        );

        let err = client
            .move_items_batch(
                &ShareId::new(FROM_SHARE.into()),
                &[ItemId::new("old-1".into())],
                &ShareId::new(TO_SHARE.into()),
                50,
                4,
            )
            .await
            .expect_err("should surface Proton error");
        let msg = format!("{err:#}");
        assert!(msg.contains("ResourceLimitExceeded"), "msg={msg}");
        assert!(msg.contains("Vault limit 5000"), "msg={msg}");
    }
}
