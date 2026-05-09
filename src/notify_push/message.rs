use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdatedFiles {
    Unknown,
    Known(BTreeSet<i64>),
}

impl UpdatedFiles {
    pub fn one(file_id: i64) -> Self {
        Self::Known(BTreeSet::from([file_id]))
    }

    pub fn extend(&mut self, more: &UpdatedFiles) {
        match (self, more) {
            (UpdatedFiles::Known(items), UpdatedFiles::Known(more)) => {
                items.extend(more.iter().copied());
            }
            (this, _) => *this = UpdatedFiles::Unknown,
        }
    }

    fn as_vec(&self) -> Option<Vec<i64>> {
        match self {
            Self::Known(items) => Some(items.iter().copied().collect()),
            Self::Unknown => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushMessage {
    File(UpdatedFiles),
    Activity,
    Notification,
    Custom {
        message: String,
        body: Option<String>,
    },
}

impl PushMessage {
    pub fn file(file_id: Option<i64>) -> Self {
        match file_id {
            Some(file_id) => Self::File(UpdatedFiles::one(file_id)),
            None => Self::File(UpdatedFiles::Unknown),
        }
    }

    pub fn merge(&mut self, other: &PushMessage) -> bool {
        match (self, other) {
            (Self::File(files), Self::File(more)) => {
                files.extend(more);
                true
            }
            _ => false,
        }
    }

    pub fn to_wire_text(&self, listen_file_id: bool) -> String {
        match self {
            Self::File(files) if listen_file_id => files.as_vec().map_or_else(
                || "notify_file".to_owned(),
                |ids| format!("notify_file_id {}", serde_json::to_string(&ids).unwrap()),
            ),
            Self::File(_) => "notify_file".to_owned(),
            Self::Activity => "notify_activity".to_owned(),
            Self::Notification => "notify_notification".to_owned(),
            Self::Custom { message, body } => match body {
                Some(body) => format!("{message} {body}"),
                None => message.clone(),
            },
        }
    }

    pub fn message_type(&self) -> MessageType {
        match self {
            Self::File(_) => MessageType::File,
            Self::Activity => MessageType::Activity,
            Self::Notification => MessageType::Notification,
            Self::Custom { .. } => MessageType::Custom,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    File,
    Activity,
    Notification,
    Custom,
}

#[cfg(test)]
mod tests {
    use super::{PushMessage, UpdatedFiles};

    #[test]
    fn file_messages_merge_and_render_ids() {
        let mut first = PushMessage::file(Some(2));
        assert!(first.merge(&PushMessage::file(Some(1))));
        assert_eq!(first.to_wire_text(true), "notify_file_id [1,2]");
        assert_eq!(first.to_wire_text(false), "notify_file");
    }

    #[test]
    fn unknown_file_message_downgrades_to_notify_file() {
        let mut first = PushMessage::file(Some(2));
        assert!(first.merge(&PushMessage::File(UpdatedFiles::Unknown)));
        assert_eq!(first.to_wire_text(true), "notify_file");
    }
}
