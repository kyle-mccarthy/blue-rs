use crossbeam::channel::{unbounded, Receiver, Sender};
use crossbeam::utils::Backoff;
use failure::{format_err, Error};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use std::thread;
use std::thread::{sleep, JoinHandle};

use crate::job::{Job, JobStatus};
use crate::{Scheduler, SchedulerMessage, Worker, WorkerMessage};
use std::time::Duration;

pub type ProcessorFn<P: Sized + Send + Sync + Clone, R: Sized + Send + Sync + Clone> =
    fn(job: &mut Job<P, R>) -> Result<&Job<P, R>, Error>;

#[derive(Clone)]
pub enum Event<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    Enqueue(Job<P, R>),
    Shutdown(&'static str),
}

#[derive(Clone)]
pub struct ManagerChannels<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    pub tx: Sender<Event<P, R>>,
    pub rx: Receiver<Event<P, R>>,
}

#[derive(Clone)]
pub struct Manager<P, R>
where
    P: Sized + Send + Sync + Clone,
    R: Sized + Send + Sync + Clone,
{
    inner: Arc<RwLock<AtomicManager>>,
    channels: ManagerChannels<P, R>,
    processor: Option<Arc<RwLock<ProcessorFn<P, R>>>>,
}

#[derive(PartialEq, Clone)]
enum ManagerState {
    DEFAULT,
    STARTING,
    STARTED,
    STOPPING,
    STOPPED,
}

#[derive(PartialEq, Clone)]
enum ProcessorState {
    DEFAULT,
    RUNNING,
    PAUSED,
}

#[derive(Clone)]
pub struct AtomicManager {
    name: &'static str,
    concurrency: usize,
    state: ManagerState,
    processor_state: ProcessorState,
}

impl<P, R> Manager<P, R>
where
    P: Sized + Send + Sync + Clone + 'static,
    R: Sized + Send + Sync + Clone + 'static,
{
    pub fn new(name: &'static str) -> Self {
        let (tx, rx): (Sender<Event<P, R>>, Receiver<Event<P, R>>) = unbounded();
        Manager {
            inner: Arc::new(RwLock::new(AtomicManager {
                state: ManagerState::DEFAULT,
                processor_state: ProcessorState::DEFAULT,
                concurrency: 1,
                name: "default",
            })),
            channels: ManagerChannels { rx, tx },
            processor: None,
        }
    }

    pub fn add_job(&self, mut job: Job<P, R>) -> uuid::Uuid {
        job.status = JobStatus::WAITING;
        let id = job.uuid.clone();
        self.channels.tx.send(Event::Enqueue(job)).unwrap();
        id
    }

    pub fn register_processor(&mut self, processor: ProcessorFn<P, R>) {
        self.processor = Some(Arc::new(RwLock::new(processor)));
    }

    pub fn can_start(&self) -> bool {
        self.inner.read().unwrap().state == ManagerState::DEFAULT
    }

    pub fn is_started(&self) -> bool {
        self.inner.read().unwrap().state == ManagerState::STARTED
    }

    pub fn is_stopped(&self) -> bool {
        self.inner.read().unwrap().state == ManagerState::STOPPED
    }

    pub fn is_running(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.state == ManagerState::STARTED && inner.processor_state == ProcessorState::RUNNING
    }

    pub fn is_paused(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.state == ManagerState::STARTED && inner.processor_state == ProcessorState::PAUSED
    }

    pub fn start(&mut self) -> Result<(), Error> {
        let state = self.inner.read().unwrap().clone().state;
        match state {
            ManagerState::DEFAULT => self.run(),
            ManagerState::STOPPED => Err(format_err!("Cannot restart once stopped")),
            _ => Err(format_err!("Cannot invoke start while manager is running")),
        }
    }

    pub fn stop(&mut self) -> Result<(), Error> {
        if self.is_started() {
            self.mark_stopping();

            self.channels
                .tx
                .send(Event::Shutdown("shutdown triggered by stop"))
                .unwrap();

            //            if let Some(handle) = self.join_handle.take() {
            //                handle.join().unwrap();
            //            }
            self.mark_stopped()
        }
        Ok(())
    }

    pub fn pause(&mut self) -> Result<(), Error> {
        if self.is_started() {
            if self.is_running() {
                self.mark_paused();
                return Ok(());
            }
            return Err(format_err!(
                "The processor cannot be paused unless it is running"
            ));
        }
        Err(format_err!(
            "The processor cannot be paused unless manager has been started and initialized"
        ))
    }

    pub fn resume(&mut self) -> Result<(), Error> {
        if self.is_started() {
            if self.is_paused() {
                self.mark_running();
                return Ok(());
            }
            return Err(format_err!(
                "The processor cannot be resumed while already running"
            ));
        }
        Err(format_err!(
            "The processor cannot be resumed unless manager has been started and initialized"
        ))
    }

    pub fn run(&mut self) -> Result<(), Error> {
        if !self.can_start() {
            return Err(format_err!("Manager not in default state, cannot start"));
        }

        self.mark_starting();

        let inner = self.inner.to_owned();
        let processor = self
            .processor
            .take()
            .expect("Must register processor before starting");

        // the main channels on the struct function as a way for the primary process to send
        // jobs to the "handler" for distribution to the multiple workers

        let master_rx = self.channels.rx.clone();

        thread::spawn(move || {
            let manager = inner.clone();
            let mut scheduler = Scheduler::<P, R>::new();
            let processor = processor.read().unwrap();

            let mut handles: Vec<JoinHandle<()>> = vec![];

            let (tx_scheduler, rx_scheduler): (
                Sender<WorkerMessage<P, R>>,
                Receiver<WorkerMessage<P, R>>,
            ) = unbounded();

            let worker: Arc<RwLock<Worker<P, R>>> = Worker::new(tx_scheduler);

            let rx_worker = scheduler.register_worker(worker.clone());
            let worker = worker.clone();

            handles.push(thread::spawn(move || {
                let backoff = Backoff::new();

                // while not shutting down or stopped, keep the thread
                while manager.read().unwrap().state != ManagerState::STOPPING
                    && manager.read().unwrap().state != ManagerState::STOPPED
                {
                    // if running should look for jobs
                    if manager.read().unwrap().processor_state == ProcessorState::RUNNING {
                        // attempt to first get the worker as writable, if success check for any new
                        // message from the scheduler and handle them accordingly
                        if let Ok(mut worker) = worker.try_write() {
                            if let Ok(message) = rx_worker.try_recv() {
                                match message {
                                    SchedulerMessage::Job(job) => {
                                        println!("got new job from scheduler");
                                        worker.set_job(job);
                                    }
                                    SchedulerMessage::AckJob(uuid) => {
                                        worker.process_job();
                                    }
                                }
                            } else if worker.is_idle() {
                                // no pending messages and the worker is idle -- ask the scheduler for a job
                                worker.request_job();
                            }
                        }

                        sleep(Duration::from_millis(250));
                        backoff.reset();
                    }

                    // if no job spin
                    backoff.spin();
                }
            }));

            let backoff = Backoff::new();
            let mut running = true;
            while running {
                if let Ok(message) = master_rx.try_recv() {
                    match message {
                        Event::Shutdown(_) => {
                            println!("shutdown signal received");
                            running = false;
                        }
                        Event::Enqueue(job) => {
                            scheduler.add_job(job);
                            backoff.reset();
                        }
                    }
                }

                if let Ok(message) = rx_scheduler.try_recv() {
                    match message {
                        WorkerMessage::JobRequest(uuid) => {
                            scheduler.handle_job_requested(uuid);
                        }
                        WorkerMessage::JobCompleted(job) => {
                            // @todo
                        }
                        WorkerMessage::AckAssignment(worker_uuid, job_uuid) => {
                            scheduler.handle_job_assignment_ack(worker_uuid, job_uuid);
                        }
                    }
                }

                backoff.spin();
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });

        self.mark_started();
        self.mark_running();

        Ok(())
    }

    fn set_state(&mut self, state: ManagerState) {
        let mut inner = self.inner.write().unwrap();
        inner.state = state;
    }

    fn mark_starting(&mut self) {
        self.set_state(ManagerState::STARTING);
    }

    fn mark_started(&mut self) {
        self.set_state(ManagerState::STARTED);
    }

    fn mark_stopping(&mut self) {
        self.set_state(ManagerState::STOPPING);
    }

    fn mark_stopped(&mut self) {
        self.set_state(ManagerState::STOPPED);
    }

    fn set_processor_state(&mut self, state: ProcessorState) {
        let mut inner = self.inner.write().unwrap();
        inner.processor_state = state;
    }

    fn mark_running(&mut self) {
        self.set_processor_state(ProcessorState::RUNNING);
    }

    fn mark_paused(&mut self) {
        self.set_processor_state(ProcessorState::PAUSED);
    }
}
