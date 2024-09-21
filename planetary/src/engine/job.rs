//! Jobs run by the engine.

use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::batch::v1::Job as KubeJob;
use k8s_openapi::api::batch::v1::JobSpec;
use k8s_openapi::api::core::v1::Container;
use k8s_openapi::api::core::v1::PodSpec;
use k8s_openapi::api::core::v1::PodTemplateSpec;
use k8s_openapi::api::core::v1::ResourceRequirements as K8sResources;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::Api;
use kube::api::DeleteParams;
use kube::api::ObjectMeta;
use kube::api::PostParams;
use tes::v1::types::Task;
use tes::v1::types::task::Executor;
use tes::v1::types::task::Resources as TesResources;
use tracing::debug;

/// The default amount of time to sleep between sleepable operations in seconds.
pub const DEFAULT_SLEEP_SECONDS: u64 = 1;

/// The default number of tries
pub const DEFAULT_RETRIES: i32 = 3;

/// Gets the K8s resource key for the requested number of CPUs.
pub static K8S_KEY_CPU: &str = "cpu";

/// Gets the K8s resource key for the requested amount of RAM.
pub static K8S_KEY_MEMORY: &str = "memory";

/// An error related to a [`Job`].
#[derive(Debug)]
pub enum Error {
    /// Failed to create a job.
    Creation(kube::Error),

    /// Failed to delete a job.
    Deletion(kube::Error),

    /// Failed to get the status of a job.
    StatusCheck(kube::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Creation(err) => write!(f, "job creation error: {err}"),
            Error::Deletion(err) => write!(f, "job deletion error: {err}"),
            Error::StatusCheck(err) => write!(f, "job status check error: {err}"),
        }
    }
}

impl std::error::Error for Error {}

/// A [`Result`](std::result::Result) with an [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// A job executed by the [`Engine`](crate::Engine).
pub struct Job {
    /// The index in terms of executors within a TES task.
    index: usize,

    /// The name given to the Kubernetes job.
    name: String,

    /// The constructed Kubernetes job.
    inner: KubeJob,
}

/// Utility methods for resources.
struct Resources;

impl Resources {
    /// Gets Kubernetes resource requirements from TES resource requests.
    pub fn from(value: &TesResources) -> K8sResources {
        let mut requests = BTreeMap::new();

        if let Some(cores) = value.cpu_cores {
            requests.insert(K8S_KEY_CPU.to_owned(), Quantity(cores.to_string()));
        }

        if let Some(memory) = value.ram_gb {
            requests.insert(K8S_KEY_MEMORY.to_owned(), Quantity(memory.to_string()));
        }

        let requests = if !requests.is_empty() {
            Some(requests)
        } else {
            None
        };

        K8sResources {
            requests,
            ..Default::default()
        }
    }
}

impl Job {
    /// Creates a new [`Job`] from an [`Executor`] and some other supporting
    /// information.
    pub fn new(
        index: usize,
        name: String,
        resources: Option<K8sResources>,
        executor: &Executor,
    ) -> Self {
        let metadata = ObjectMeta {
            name: Some(name.clone()),
            ..Default::default()
        };

        let spec = JobSpec {
            backoff_limit: Some(DEFAULT_RETRIES),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    name: Some(format!("{name}-pod")),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: format!("{name}-container"),
                        image: Some(executor.image.clone()),
                        args: Some(executor.command.clone()),
                        resources: resources.clone(),
                        ..Default::default()
                    }],
                    restart_policy: Some(String::from("Never")),
                    ..Default::default()
                }),
            },
            ..Default::default()
        };

        Self {
            index,
            name,
            inner: KubeJob {
                metadata,
                spec: Some(spec),
                ..Default::default()
            },
        }
    }

    /// Gets the execution index.
    pub fn execution_index(&self) -> usize {
        self.index
    }

    /// Gets the name of the job.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets the inner Kubernetes job.
    pub fn inner(&self) -> &KubeJob {
        &self.inner
    }
}

/// A set of [`Job`] that should be executed sequentially.
pub struct JobGroup {
    /// The name.
    name: String,

    /// The jobs.
    jobs: Vec<Job>,
}

impl JobGroup {
    /// Creates a new [`JobGroup`].
    pub fn new(name: String, task: &Task) -> Self {
        let resources = task.resources.as_ref().map(Resources::from);

        let jobs = task
            .executors
            .iter()
            .enumerate()
            .map(|(index, executor)| {
                let name = format!("{name}-{index}");
                Job::new(index, name, resources.clone(), executor)
            })
            .collect::<Vec<_>>();

        Self { name, jobs }
    }

    /// Gets the name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets the jobs.
    pub fn jobs(&self) -> &[Job] {
        &self.jobs
    }

    /// Runs the jobs in the group sequentially.
    pub async fn run(&self, api: Arc<Api<KubeJob>>) -> Result<()> {
        debug!(
            "queued task `{}` (contains {} jobs)",
            self.name,
            self.jobs.len()
        );

        for job in &self.jobs {
            debug!("submitting job: {}", job.name());

            api.create(&PostParams::default(), job.inner())
                .await
                .map_err(Error::Creation)?;

            loop {
                let job = api.get(job.name()).await.map_err(Error::StatusCheck)?;

                if let Some(status) = job.status {
                    if status.succeeded > Some(0) {
                        debug!("job finished successfully: {status:?}");
                        break;
                    }
                }

                tokio::time::sleep(Duration::from_secs(DEFAULT_SLEEP_SECONDS)).await;
            }
        }

        Ok(())
    }

    /// Removes all jobs associated within this job group.
    pub async fn remove_all(&self, api: Arc<Api<KubeJob>>) -> Result<()> {
        debug!(
            "removing task `{}` (contains {} jobs)",
            self.name,
            self.jobs.len()
        );

        for job in &self.jobs {
            debug!("removing job: {}", job.name());

            api.delete(&job.name, &DeleteParams::default())
                .await
                .map_err(Error::Creation)?;
        }

        Ok(())
    }
}

impl Deref for JobGroup {
    type Target = Vec<Job>;

    fn deref(&self) -> &Self::Target {
        &self.jobs
    }
}

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;

    use super::*;

    #[test]
    fn resource_mapping_works_for_full_resources() {
        let tes = TesResources {
            cpu_cores: Some(1),
            preemptible: Some(true),
            ram_gb: Some(OrderedFloat(4.)),
            disk_gb: Some(OrderedFloat(16.)),
            zones: Some(vec![String::from("foobar")]),
        };

        let k8s = Resources::from(&tes).requests.unwrap();

        let cpu = &k8s.get(K8S_KEY_CPU).unwrap().0;
        assert_eq!(cpu, "1");

        let ram = &k8s.get(K8S_KEY_MEMORY).unwrap().0;
        assert_eq!(ram, "4");
    }
}
