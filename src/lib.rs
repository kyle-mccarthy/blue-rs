pub mod job;
pub mod manager;
pub mod scheduler;
pub mod worker;

pub use job::*;
pub use manager::*;
pub use scheduler::*;
pub use worker::*;

#[cfg(test)]
mod tests {
    use super::*;
    use super::{Job, Manager};
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn it_can_run_manager() {
        let mut manager = Manager::<String, String>::new("test-manager");

        let processor: ProcessorFn<String, String> = |job| Ok(job);
        manager.register_processor(processor);

        let start_result = manager.start();
        assert!(start_result.is_ok());

        let job = Job::<String, String>::new("test", String::from("test"));
        manager.add_job(job);

        sleep(Duration::from_secs(3));

        let stop_result = manager.stop();
        assert!(stop_result.is_ok());
    }
}
