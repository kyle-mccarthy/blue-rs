use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub enum JobStatus {
    IDLE,
    WAITING,
    ACTIVE,
    COMPLETED,
    ERRORED,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Metadata {
    pub created_at: SystemTime,
    pub updated_at: Option<SystemTime>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Job<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    pub uuid: Uuid,
    pub status: JobStatus,
    pub payload: P,
    pub response: Option<R>,
    pub processor_name: &'static str,
    pub metadata: Metadata,
}

impl<P, R> Job<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    pub fn new(processor_name: &'static str, payload: P) -> Job<P, R> {
        Job {
            processor_name,
            payload,
            status: JobStatus::IDLE,
            response: None,
            uuid: Uuid::new_v4(),
            metadata: Metadata {
                created_at: SystemTime::now(),
                updated_at: None,
            },
        }
    }
}
