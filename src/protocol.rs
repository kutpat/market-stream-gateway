use serde::{Deserialize, Serialize};

use crate::domain::{SCHEMA_VERSION, Subscription, SubscriptionKey};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientCommand {
    Subscribe {
        request_id: String,
        subscriptions: Vec<Subscription>,
    },
    Unsubscribe {
        request_id: String,
        subscriptions: Vec<Subscription>,
    },
    Ping {
        request_id: String,
    },
}

impl ClientCommand {
    pub fn request_id(&self) -> &str {
        match self {
            Self::Subscribe { request_id, .. }
            | Self::Unsubscribe { request_id, .. }
            | Self::Ping { request_id } => request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    Hello {
        schema_version: u16,
        stream_epoch: String,
        max_subscriptions: usize,
    },
    Ack {
        schema_version: u16,
        request_id: String,
        operation: String,
        subscriptions: Vec<SubscriptionKey>,
    },
    Error {
        schema_version: u16,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        code: ErrorCode,
        message: String,
    },
    Pong {
        schema_version: u16,
        request_id: String,
    },
    Gap {
        schema_version: u16,
        dropped_messages: u64,
        message: String,
    },
}

impl ControlMessage {
    pub fn hello(stream_epoch: impl Into<String>, max_subscriptions: usize) -> Self {
        Self::Hello {
            schema_version: SCHEMA_VERSION,
            stream_epoch: stream_epoch.into(),
            max_subscriptions,
        }
    }

    pub fn error(request_id: Option<String>, code: ErrorCode, message: impl Into<String>) -> Self {
        Self::Error {
            schema_version: SCHEMA_VERSION,
            request_id,
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidJson,
    InvalidCommand,
    LimitExceeded,
    UnsupportedSubscription,
    Lagged,
    Internal,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn commands_reject_unknown_fields() {
        let invalid = json!({"op":"ping", "request_id":"p1", "extra":true});
        assert!(serde_json::from_value::<ClientCommand>(invalid).is_err());
    }

    #[test]
    fn control_messages_carry_schema_version() {
        let message = ControlMessage::hello("epoch", 100);
        let value = serde_json::to_value(message).unwrap();
        assert_eq!(value["type"], "hello");
        assert_eq!(value["schema_version"], SCHEMA_VERSION);
    }
}
