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

use super::common::CreateItemRequest;
use crate::item::list::ItemRevision;
use crate::{PassClient, PassClientContext};
use anyhow::{Context, Result, anyhow};
use muon::POST;
use pass_domain::{ItemData, ItemId, ShareId};

const MAX_ITEMS_PER_BATCH: usize = 100;

#[derive(serde::Deserialize, serde::Serialize)]
struct BatchImportItemBody {
    #[serde(rename = "Item")]
    item: CreateItemRequest,
    #[serde(rename = "AliasEmail")]
    alias_email: Option<String>,
    #[serde(rename = "Trashed")]
    trashed: bool,
    #[serde(rename = "CreateTime")]
    create_time: Option<u64>,
    #[serde(rename = "ModifyTime")]
    modify_time: Option<u64>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct BatchCreateItemsRequest {
    #[serde(rename = "Items")]
    items: Vec<BatchImportItemBody>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct BatchCreateItemsResponse {
    #[serde(rename = "Revisions")]
    revisions: BatchItemRevisions,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct BatchItemRevisions {
    #[serde(rename = "RevisionsData")]
    items: Vec<ItemRevision>,
}

impl<C: PassClientContext> PassClient<C> {
    pub async fn create_items(
        &self,
        share_id: &ShareId,
        items: Vec<ItemData>,
    ) -> Result<Vec<ItemId>> {
        let mut all_ids = vec![];
        for chunk in items.chunks(MAX_ITEMS_PER_BATCH) {
            let ids = self.create_items_batch(share_id, chunk).await?;
            all_ids.extend(ids);
        }
        self.clear_items_cache(share_id).await;
        Ok(all_ids)
    }

    async fn create_items_batch(
        &self,
        share_id: &ShareId,
        items: &[ItemData],
    ) -> Result<Vec<ItemId>> {
        let mut import_bodies = vec![];
        for item in items {
            let req = self
                .create_item_request_from_data(share_id, item.clone(), None)
                .await
                .context("Error creating item request")?;
            import_bodies.push(BatchImportItemBody {
                item: req,
                alias_email: None,
                trashed: false,
                create_time: None,
                modify_time: None,
            });
        }

        let request = BatchCreateItemsRequest {
            items: import_bodies,
        };
        let res = POST!("/pass/v1/share/{share_id}/item/import/batch")
            .body_json(request)
            .context("Error serializing batch create items request")?;
        let response = self
            .send(res)
            .await
            .context("Error sending batch create items request")?;
        let response: BatchCreateItemsResponse = assert_response!(response);
        let ids: Vec<ItemId> = response
            .revisions
            .items
            .into_iter()
            .map(|r| ItemId::new(r.item_id))
            .collect();
        if ids.len() != items.len() {
            return Err(anyhow!(
                "Batch create returned {} item IDs for {} input items",
                ids.len(),
                items.len()
            ));
        }

        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_tools::*;

    #[muon_test::test]
    async fn test_create_items_chunks_batches_at_100(server: muon_test::Server) {
        let (raw_client, api) = server.client::<()>();
        const SHARE_ID: &str = "BatchShareID";
        let client = make_test_pass_client_with_setup(raw_client, &api, PlanType::Free).await;
        setup_share_keys(&api, SHARE_ID);
        setup_vault_share(&api, SHARE_ID);

        let batch_path = format!("/pass/v1/share/{SHARE_ID}/item/import/batch");
        // muon-test-server removes a registered handler after it matches once,
        // so register one one-shot responder per expected batch request.
        let handled_first = api.handler_with_method(Method::POST, batch_path.clone(), move |_| {
            let revs = (1..=100)
                .map(|n| ItemRevisionBuilder::new(format!("Item-{n}")).build())
                .collect();
            success(BatchCreateItemsResponse {
                revisions: BatchItemRevisions { items: revs },
            })
        });
        let handled_second = api.handler_with_method(Method::POST, batch_path.clone(), move |_| {
            let revs = (101..=105)
                .map(|n| ItemRevisionBuilder::new(format!("Item-{n}")).build())
                .collect();
            success(BatchCreateItemsResponse {
                revisions: BatchItemRevisions { items: revs },
            })
        });

        let recorder = api.new_recorder();
        let items: Vec<ItemData> = (0..105).map(|_| create_random_item()).collect();
        let ids = client
            .create_items(&share_id!(SHARE_ID), items)
            .await
            .expect("batch create should succeed");

        assert_hit!(handled_first);
        assert_hit!(handled_second);
        let batch_reqs: Vec<BatchCreateItemsRequest> = recorder
            .read()
            .into_iter()
            .filter(|r| r.uri().path() == batch_path)
            .map(|r| serde_json::from_slice(&r.body().to_vec()).unwrap())
            .collect();
        assert_eq!(batch_reqs.len(), 2);
        assert_eq!(batch_reqs[0].items.len(), 100);
        assert_eq!(batch_reqs[1].items.len(), 5);
        assert_eq!(ids.len(), 105);
        assert_eq!(ids[0].value(), "Item-1");
        assert_eq!(ids[104].value(), "Item-105");
    }
}
