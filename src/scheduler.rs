use crossbeam::channel::{unbounded, Receiver, Sender};
use hashbrown::HashMap;
use std::collections::vec_deque::VecDeque;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

use crate::job::{Job, JobStatus};
use crate::worker::Worker;
use std::time::SystemTime;

pub enum SchedulerMessage<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    Job(Job<P, R>),
    AckJob(Uuid),
}

// @todo add hash map that stores all the jobs, make queue store reference to job uuid
// @todo add in_progress queue, possibly awaiting ACK queue too? Timeout after some x?

pub struct Scheduler<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    pub waiting_queue: VecDeque<Uuid>,
    pub ack_queue: Vec<Uuid>,
    pub active_queue: Vec<Uuid>,
    pub completed_queue: Vec<Uuid>,
    pub workers: HashMap<Uuid, Arc<RwLock<Worker<P, R>>>>,
    pub channels: HashMap<Uuid, Sender<SchedulerMessage<P, R>>>,
    pub jobs: HashMap<Uuid, Job<P, R>>,
}

impl<P, R> Default for Scheduler<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    fn default() -> Scheduler<P, R> {
        Scheduler {
            waiting_queue: VecDeque::new(),
            workers: HashMap::new(),
            channels: HashMap::new(),
            jobs: HashMap::new(),
            ack_queue: vec![],
            active_queue: vec![],
            completed_queue: vec![],
        }
    }
}

impl<P, R> Scheduler<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    pub fn new() -> Scheduler<P, R> {
        Scheduler {
            ..Default::default()
        }
    }

    pub fn register_worker(
        &mut self,
        atomic_worker: Arc<RwLock<Worker<P, R>>>,
    ) -> Receiver<SchedulerMessage<P, R>> {
        let worker = atomic_worker.read().unwrap();
        let (tx, rx): (
            Sender<SchedulerMessage<P, R>>,
            Receiver<SchedulerMessage<P, R>>,
        ) = unbounded();

        self.workers.insert(worker.uuid, atomic_worker.clone());
        self.channels.insert(worker.uuid, tx);

        rx
    }

    pub fn add_job(&mut self, mut job: Job<P, R>) {
        job.status = JobStatus::WAITING;
        let id = job.uuid.clone();
        self.jobs.insert(job.uuid, job);
        self.waiting_queue.push_back(id);
    }

    pub fn handle_job_requested(&mut self, uuid: Uuid) {
        if !self.waiting_queue.is_empty() {
            if let Some(tx) = self.channels.get(&uuid) {
                if let Some(id) = self.waiting_queue.pop_front() {
                    if let Some(job) = self.jobs.get_mut(&id) {
                        job.metadata.updated_at = Some(SystemTime::now());
                        let mut job = (*job).clone();
                        self.ack_queue.push(id);
                        tx.send(SchedulerMessage::Job(job)).unwrap();
                    }
                }
            }
        }
    }

    pub fn handle_job_assignment_ack(&mut self, worker_uuid: Uuid, job_uuid: Uuid) {
        if self.ack_queue.contains(&job_uuid) {
            if let Some(tx) = self.channels.get(&worker_uuid) {
                if let Some(job) = self.jobs.get_mut(&job_uuid) {
                    job.metadata.updated_at = Some(SystemTime::now());

                    let pos = self.ack_queue.iter().position(|item| item == &job_uuid);

                    if let Some(pos) = pos {
                        self.ack_queue.remove(pos);
                    }

                    self.active_queue.push(job_uuid);
                    tx.send(SchedulerMessage::AckJob(job_uuid));
                }
            }
        }
    }
}
