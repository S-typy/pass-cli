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

use crate::{PassClient, PassClientContext};
use anyhow::{Context, Result};
use muon::GET;
use pass_domain::{
    ContinuationStrategy, EventId, FolderId, ItemId, ShareId, SyncEventChangedWithToken,
    SyncEventShare, SyncEventShareFolder, SyncEventShareItem, UserEvents, UserEventsHandler,
};
use std::sync::Arc;

#[derive(Debug, serde::Deserialize)]
struct GetLastEventIdResponse {
    #[serde(rename = "EventID")]
    event_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct SyncEventChangedWithTokenOutput {
    #[serde(rename = "EventToken")]
    pub event_token: String,
}

#[derive(Debug, serde::Deserialize)]
struct SyncEventShareFolderOutput {
    #[serde(rename = "ShareID")]
    pub share_id: String,
    #[serde(rename = "FolderID")]
    pub folder_id: String,
    #[serde(rename = "EventToken")]
    pub event_token: String,
}

#[derive(Debug, serde::Deserialize)]
struct SyncEventShareOutput {
    #[serde(rename = "ShareID")]
    pub share_id: String,
    #[serde(rename = "EventToken")]
    pub event_token: String,
}

#[derive(Debug, serde::Deserialize)]
struct SyncEventShareItemOutput {
    #[serde(rename = "ShareID")]
    pub share_id: String,
    #[serde(rename = "ItemID")]
    pub item_id: String,
    #[serde(rename = "EventToken")]
    pub event_token: String,
}

#[derive(Debug, serde::Deserialize)]
struct Events {
    #[serde(rename = "LastEventID")]
    pub last_event_id: String,
    #[serde(rename = "ItemsUpdated", default)]
    pub items_updated: Vec<SyncEventShareItemOutput>,
    #[serde(rename = "ItemsDeleted", default)]
    pub items_deleted: Vec<SyncEventShareItemOutput>,
    #[serde(rename = "AliasNoteChanged", default)]
    pub alias_note_changed: Vec<SyncEventShareItemOutput>,
    #[serde(rename = "SharesCreated", default)]
    pub shares_created: Vec<SyncEventShareOutput>,
    #[serde(rename = "SharesUpdated", default)]
    pub shares_updated: Vec<SyncEventShareOutput>,
    #[serde(rename = "SharesDeleted", default)]
    pub shares_deleted: Vec<SyncEventShareOutput>,
    #[serde(rename = "FoldersUpdated", default)]
    pub folders_updated: Vec<SyncEventShareFolderOutput>,
    #[serde(rename = "FoldersDeleted", default)]
    pub folders_deleted: Vec<SyncEventShareFolderOutput>,
    #[serde(rename = "InvitesChanged", default)]
    pub invites_changed: Option<SyncEventChangedWithTokenOutput>,
    #[serde(rename = "GroupInvitesChanged", default)]
    pub group_invites_changed: Option<SyncEventChangedWithTokenOutput>,
    #[serde(rename = "PendingAliasToCreateChanged", default)]
    pub pending_alias_to_create_changed: Option<SyncEventChangedWithTokenOutput>,
    #[serde(rename = "BreachUpdate", default)]
    pub breach_update: Option<SyncEventChangedWithTokenOutput>,
    #[serde(rename = "OrganizationInfoChanged", default)]
    pub organization_info_changed: Option<SyncEventChangedWithTokenOutput>,
    #[serde(rename = "SharesWithInvitesToCreate", default)]
    pub shares_with_invites_to_create: Vec<SyncEventShareOutput>,
    #[serde(rename = "RefreshUser", default)]
    pub refresh_user: bool,
    #[serde(rename = "EventsPending", default)]
    pub events_pending: bool,
    #[serde(rename = "FullRefresh", default)]
    pub full_refresh: bool,
}

#[derive(Debug, serde::Deserialize)]
struct GetEventsResponse {
    #[serde(rename = "Events")]
    pub events: Events,
}

impl<C: PassClientContext> PassClient<C> {
    pub async fn get_last_user_event_id(&self) -> Result<EventId> {
        self.request_last_event_id()
            .await
            .context("Error fetching last user event id")
    }

    pub async fn get_user_events(&self, event_id: &EventId) -> Result<UserEvents> {
        self.fetch_events(event_id)
            .await
            .map(UserEvents::from)
            .context("Error fetching user events")
    }

    pub async fn listen_for_events(&self, handler: Arc<dyn UserEventsHandler>) -> Result<()> {
        let initial_event = handler
            .get_last_user_event_id()
            .await
            .context("Error getting last user event id")?;
        let event = match initial_event {
            Some(event) => event,
            None => {
                let event = self
                    .request_last_event_id()
                    .await
                    .context("Error fetching last user event id")?;
                handler
                    .set_last_user_event_id(event.clone())
                    .await
                    .context("Error storing last user event id")?;
                event
            }
        };
        self.start_listening(event, handler)
            .await
            .context("Error while listening for events")
    }

    async fn start_listening(
        &self,
        event_id: EventId,
        handler: Arc<dyn UserEventsHandler>,
    ) -> Result<()> {
        let mut event_id = event_id;

        loop {
            // Fetch new events
            let events = match self.fetch_events(&event_id).await {
                Ok(events) => {
                    handler.on_event_fetch_success().await;
                    events
                }
                Err(e) => {
                    let strategy = handler
                        .on_error(e)
                        .await
                        .context("Error on UserEventsHandler.on_error")?;
                    match strategy {
                        ContinuationStrategy::Continue => continue,
                        ContinuationStrategy::Break { err } => return Err(err),
                    }
                }
            };

            // Check if there are new events
            if events.last_event_id != event_id.value() {
                // Convert API events to domain events
                let user_events = UserEvents::from(events);

                // Tell handler to update the last event ID
                handler
                    .set_last_user_event_id(user_events.last_event_id.clone())
                    .await
                    .context("Error setting last user event id")?;

                // Notify handler with the events
                handler
                    .on_event(user_events.clone())
                    .await
                    .context("Error on events handler")?;

                // Update our local last event ID
                event_id = user_events.last_event_id;
            }

            // Wait until the next time
            handler.tick().await;
        }
    }

    async fn fetch_events(&self, event_id: &EventId) -> Result<Events> {
        let res = self
            .send(GET!("/pass/v1/user/sync_event/{event_id}"))
            .await
            .context("Error fetching events")?;
        let response: GetEventsResponse = assert_response!(res);

        Ok(response.events)
    }

    async fn request_last_event_id(&self) -> Result<EventId> {
        let res = self
            .send(GET!("/pass/v1/user/sync_event"))
            .await
            .context("Error retrieving last user event id")?;
        let response: GetLastEventIdResponse = assert_response!(res);

        Ok(EventId::new(response.event_id))
    }
}

// From implementations to convert API response types to domain types

impl From<SyncEventChangedWithTokenOutput> for SyncEventChangedWithToken {
    fn from(value: SyncEventChangedWithTokenOutput) -> Self {
        Self {
            event_token: EventId::new(value.event_token),
        }
    }
}

impl From<SyncEventShareFolderOutput> for SyncEventShareFolder {
    fn from(value: SyncEventShareFolderOutput) -> Self {
        Self {
            share_id: ShareId::new(value.share_id),
            folder_id: FolderId::new(value.folder_id),
            event_token: EventId::new(value.event_token),
        }
    }
}

impl From<SyncEventShareOutput> for SyncEventShare {
    fn from(value: SyncEventShareOutput) -> Self {
        Self {
            share_id: ShareId::new(value.share_id),
            event_token: EventId::new(value.event_token),
        }
    }
}

impl From<SyncEventShareItemOutput> for SyncEventShareItem {
    fn from(value: SyncEventShareItemOutput) -> Self {
        Self {
            share_id: ShareId::new(value.share_id),
            item_id: ItemId::new(value.item_id),
            event_token: EventId::new(value.event_token),
        }
    }
}

impl From<Events> for UserEvents {
    fn from(value: Events) -> Self {
        Self {
            last_event_id: EventId::new(value.last_event_id),
            items_updated: value
                .items_updated
                .into_iter()
                .map(SyncEventShareItem::from)
                .collect(),
            items_deleted: value
                .items_deleted
                .into_iter()
                .map(SyncEventShareItem::from)
                .collect(),
            alias_note_changed: value
                .alias_note_changed
                .into_iter()
                .map(SyncEventShareItem::from)
                .collect(),
            shares_created: value
                .shares_created
                .into_iter()
                .map(SyncEventShare::from)
                .collect(),
            shares_updated: value
                .shares_updated
                .into_iter()
                .map(SyncEventShare::from)
                .collect(),
            shares_deleted: value
                .shares_deleted
                .into_iter()
                .map(SyncEventShare::from)
                .collect(),
            folders_updated: value
                .folders_updated
                .into_iter()
                .map(SyncEventShareFolder::from)
                .collect(),
            folders_deleted: value
                .folders_deleted
                .into_iter()
                .map(SyncEventShareFolder::from)
                .collect(),
            invites_changed: value.invites_changed.map(SyncEventChangedWithToken::from),
            group_invites_changed: value
                .group_invites_changed
                .map(SyncEventChangedWithToken::from),
            pending_alias_to_create_changed: value
                .pending_alias_to_create_changed
                .map(SyncEventChangedWithToken::from),
            breach_update: value.breach_update.map(SyncEventChangedWithToken::from),
            organization_info_changed: value
                .organization_info_changed
                .map(SyncEventChangedWithToken::from),
            shares_with_invites_to_create: value
                .shares_with_invites_to_create
                .into_iter()
                .map(SyncEventShare::from)
                .collect(),
            refresh_user: value.refresh_user,
            events_pending: value.events_pending,
            full_refresh: value.full_refresh,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_tools::*;
    use muon_test::server::Response;

    #[muon_test::test]
    async fn test_get_last_user_event_id_and_events(server: muon_test::Server) {
        let (raw_client, api) = server.client::<()>();
        let client = make_test_pass_client_with_setup(raw_client, &api, PlanType::Free).await;

        let last_handled = api.handler_with_method(Method::GET, "/pass/v1/user/sync_event", |_| {
            Response::builder()
                .status(200)
                .body(axum::body::Body::from(
                    r#"{"Code":1000,"EventID":"cursor-1"}"#,
                ))
                .ok()
        });
        let delta_handled =
            api.handler_with_method(Method::GET, "/pass/v1/user/sync_event/cursor-1", |_| {
                Response::builder()
                    .status(200)
                    .body(axum::body::Body::from(
                        r#"{
                            "Code":1000,
                            "Events":{
                                "LastEventID":"cursor-2",
                                "ItemsUpdated":[{"ShareID":"s1","ItemID":"i1","EventToken":"t1"}],
                                "ItemsDeleted":[{"ShareID":"s1","ItemID":"i2","EventToken":"t2"}],
                                "EventsPending":false,
                                "FullRefresh":false
                            }
                        }"#,
                    ))
                    .ok()
            });

        let cursor = client
            .get_last_user_event_id()
            .await
            .expect("should fetch last event id");
        assert_hit!(last_handled);
        assert_eq!(cursor.value(), "cursor-1");

        let events = client
            .get_user_events(&cursor)
            .await
            .expect("should fetch user events");
        assert_hit!(delta_handled);
        assert_eq!(events.last_event_id.value(), "cursor-2");
        assert_eq!(events.items_updated.len(), 1);
        assert_eq!(events.items_updated[0].item_id.value(), "i1");
        assert_eq!(events.items_deleted.len(), 1);
        assert_eq!(events.items_deleted[0].item_id.value(), "i2");
        assert!(!events.events_pending);
        assert!(!events.full_refresh);
    }
}
