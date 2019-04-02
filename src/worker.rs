use crate::job::Job;
use crate::manager::Event;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

pub enum WorkerMessage<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    JobRequest(Uuid),
    JobCompleted(Job<P, R>),
    AckAssignment(Uuid, Uuid),
}

#[derive(PartialEq, Clone)]
pub enum WorkerState {
    Idle,
    Acking,
    Working,
}

#[derive(Clone)]
pub struct Worker<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    pub uuid: Uuid,
    pub state: WorkerState,
    pub current_job: Option<Job<P, R>>,
    pub tx: Sender<WorkerMessage<P, R>>,
}

impl<P, R> Worker<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    pub fn new(global_tx: Sender<WorkerMessage<P, R>>) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Worker {
            uuid: Uuid::new_v4(),
            state: WorkerState::Idle,
            current_job: None,
            tx: global_tx,
        }))
    }

    pub fn set_job(&mut self, job: Job<P, R>) {
        self.state = WorkerState::Acking;
        self.tx
            .send(WorkerMessage::AckAssignment(self.uuid, job.uuid))
            .unwrap();
        self.current_job = Some(job);
    }

    pub fn request_job(&self) {
        self.tx.send(WorkerMessage::JobRequest(self.uuid)).unwrap();
    }

    pub fn is_idle(&self) -> bool {
        self.state == WorkerState::Idle
    }

    pub fn process_job(&mut self) {
        println!("should process job");
        self.state = WorkerState::Working;
    }
}
