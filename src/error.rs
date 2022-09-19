 
use std::{fmt, sync::Arc};


#[derive(Clone, Debug)]
pub struct Error {
    pub kind: Arc<CacheError>,
}

impl Error {
    pub fn new(kind: CacheError) -> Self {
        Error {
            kind: Arc::new(kind),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self.kind {
            CacheError::TokioSenderError(_) => None,
            CacheError::BroadcastRecvError(_) => None,
            CacheError::Unknown(_) => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)
    }
}

#[derive(Debug)]
pub enum CacheError {
    TokioSenderError(String),
    BroadcastRecvError(String),
    Unknown(String),
}

impl std::error::Error for CacheError {}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CacheError::TokioSenderError(msg) => {
                write!(f, "Failed to send on tokio channel. Error {}", msg)
            },
            CacheError::BroadcastRecvError(msg) => {
                write!(f, "Failed to receive on broadcast channel. Error {}", msg)
            },
            CacheError::Unknown(msg) => write!(f, "Unknown error {}", msg),
        }
    }
}
