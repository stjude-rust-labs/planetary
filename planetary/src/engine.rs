//! The core task execution engine.

use std::sync::Arc;

use kube::Api;
use kube::Client;
use tes::v1::types::Task as TesTask;
use tes::v1::types::responses::task;
use tes::v1::types::responses::task::MinimalTask;
use tes::v1::types::task::State as TesState;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;

use crate::task::ResultsMap;
use crate::task::StateMap;
use crate::task::TaskMap;

mod job;

pub use job::Job;
pub use job::JobGroup;

/// An engine for processing tasks submitted to the server.
pub struct Engine {
    /// The Kubernetes jobs API.
    api: Arc<Api<k8s_openapi::api::batch::v1::Job>>,

    /// All tasks.
    tasks: Arc<RwLock<TaskMap>>,

    /// States for all tasks.
    states: Arc<RwLock<StateMap>>,

    /// Results of all tasks.
    results: Arc<RwLock<ResultsMap>>,
}

impl Engine {
    /// Creates a new [`Engine`] with empty state.
    pub fn new(client: Client) -> Self {
        Self {
            api: Arc::new(Api::default_namespaced(client)),
            tasks: Default::default(),
            states: Default::default(),
            results: Default::default(),
        }
    }

    /// Queues a task to be executed.
    pub async fn queue(&self, name: String, task: TesTask) {
        // Insert into queued table.
        if self
            .tasks
            .write()
            .await
            .insert(name.clone(), task)
            .is_some()
        {
            unreachable!("name would be overwritten for job: `{name}`");
        };

        // Insert into states table.
        if self
            .states
            .write()
            .await
            .insert(name.clone(), TesState::Queued)
            .is_some()
        {
            unreachable!("name would be overwritten for job: `{name}`");
        };
    }

    /// Attempts to get the state of a task by name.
    pub async fn state(&self, id: String, view: task::View) -> Option<task::Response> {
        // NOTE: the lock is immediately released.
        let state = self.states.read().await.get(&id).copied()?;

        Some(
            construct_response(self.tasks.clone(), id, view, state)
                .await
                // SAFETY: the task was already confirmed to exist in the states
                // above, so any subsequent lookups (say, in the tasks map, as is
                // done in [`construct_task_responses`]) should succeed.
                .expect("task should always exist here"),
        )
    }

    /// Gets the state for all jobs known about.
    pub async fn all_states(&self, view: task::View) -> Vec<task::Response> {
        let futures = self
            .states
            .read()
            .await
            .iter()
            .map(|(id, state)| {
                construct_response(
                    self.tasks.clone(),
                    id.to_owned(),
                    view.clone(),
                    state.to_owned(),
                )
            })
            .collect::<Vec<_>>();

        // TODO(clay): this could probably be improved. That being said, we have
        // to wait for all futures to finish anyway, so it's not _that_ bad that
        // we don't, say, use a [`FuturesUnordered`].
        let mut results = Vec::new();

        for future in futures {
            // SAFETY: if the state is being checked, the
            results.push(
                future
                    .await
                    // SAFETY: the task was already confirmed to exist in the states
                    // above, so any subsequent lookups (say, in the tasks map, as is
                    // done in [`construct_task_responses`]) should succeed.
                    .expect("task should always exist here"),
            );
        }

        results
    }

    /// Starts the task processing engine.
    pub async fn start(&self) -> ! {
        loop {
            // First, check if there is any work to be done. Note that, in
            // this case, the number of times this will be empty is expected to
            // be _far_ greater than the number of times that tasks and queued.
            // As such, a read permit is taken first to encourage quick
            // evaluation of whether or not tasks need to be run before
            // requesting a subsequent write permit (if necessary).
            let queued = self
                .states
                .read()
                .await
                .iter()
                .filter(|(_, state)| matches!(state, TesState::Queued))
                .map(|(name, _)| name.to_owned())
                .collect::<Vec<_>>();

            if !queued.is_empty() {
                debug!("running {} queued tasks", queued.len());

                // This is done _before_ creating the [`JobGroup`]s below
                // because we want to minimize the amount of time we hold the
                // lock (we'll pay the cost of cloning/allocating a vector) to
                // avoid it.
                let queued_tasks = {
                    let _permit = self.tasks.read().await;
                    let tasks = queued
                        .into_iter()
                        .map(|name| {
                            let task = _permit
                                .get(&name)
                                .expect("task should exist if we are attempting to run it here")
                                .clone();
                            (name, task)
                        })
                        .collect::<Vec<_>>();
                    tasks
                };

                let mut groups = Vec::with_capacity(queued_tasks.len());
                for (name, task) in &queued_tasks {
                    groups.push(JobGroup::new(name.clone(), task));
                }

                // Submit all of the jobs for async execution.
                for group in groups {
                    let api = self.api.clone();
                    let states = self.states.clone();
                    let results = self.results.clone();

                    tokio::spawn(async move {
                        match group.run(api.clone()).await {
                            Ok(_) => {
                                debug!("task completed: `{}`", group.name());

                                // TODO(clay): this should insert real results here.
                                results.write().await.insert(group.name().to_owned(), ());

                                let old_state = states
                                    .write()
                                    .await
                                    .insert(group.name().to_owned(), TesState::Complete);

                                // NOTE: this should only ever move running
                                // tasks to complete.
                                debug_assert_eq!(old_state, Some(TesState::Running));

                                if let Err(err) = group.remove_all(api.clone()).await {
                                    error!("failed to delete job: {err}");
                                }
                            }
                            Err(err) => {
                                error!("error: {err}");

                                let old_state = states
                                    .write()
                                    .await
                                    .insert(group.name().to_owned(), TesState::ExecutorError);

                                // NOTE: this should only ever move running tasks to
                                // executor error.
                                debug_assert_eq!(old_state, Some(TesState::Running));

                                if let Err(err) = group.remove_all(api.clone()).await {
                                    error!("failed to delete job: {err}");
                                }
                            }
                        }
                    });
                }

                // Now that everything has been submitted to Tokio, we can bulk
                // update the states for all tasks to running.
                let mut _permit = self.states.write().await;

                for (name, _) in queued_tasks {
                    let old_state = _permit.insert(name, TesState::Running);
                    // NOTE: this should only ever move queued tasks to running.
                    debug_assert_eq!(old_state, Some(TesState::Queued));
                }

                // Although the drop is not explicitly required, I've done it
                // here to ensure that it isn't forgotten if more code is added
                // underneath.
                drop(_permit);
            }
        }
    }
}

/// Gets a [`task::Response`] from the arguments.
async fn construct_response(
    tasks: Arc<RwLock<TaskMap>>,
    id: String,
    view: task::View,
    state: TesState,
) -> Option<task::Response> {
    match view {
        task::View::Minimal => Some(task::Response::Minimal(MinimalTask {
            id,
            state: Some(state),
        })),
        task::View::Basic | task::View::Full => {
            // SAFETY: if the task exists in the state map above, it should
            // always be present in the tasks map (as it is inserted first there);
            let mut task = tasks.read().await.get(&id)?.to_owned();
            task.state = Some(state);

            if view == task::View::Basic {
                Some(task::Response::Basic(transform_to_basic_task(task)))
            } else {
                Some(task::Response::Full(task))
            }
        }
    }
}

/// Transforms a full task to what the API expects for a `BASIC` view.
fn transform_to_basic_task(mut task: TesTask) -> TesTask {
    // Requirements for basic view:
    //
    // (1) Remove `tesTask.ExecutorLog.stdout`.
    // (2) Remove `tesTask.ExecutorLog.stderr`.
    // (3) Remove `tesTask.TesInput.content`.
    // (4) Remove `tesTask.TesTaskLog.system_logs`.

    task.inputs = task.inputs.map(|inputs| {
        inputs
            .iter()
            .cloned()
            .map(|mut input| {
                input.content = None;
                input
            })
            .collect::<Vec<_>>()
    });

    task.logs = task.logs.map(|log_entries| {
        log_entries
            .into_iter()
            .map(|mut log_entry| {
                log_entry.logs = log_entry
                    .logs
                    .into_iter()
                    .map(|mut log| {
                        log.stdout = None;
                        log.stderr = None;
                        log
                    })
                    .collect::<Vec<_>>();
                log_entry.system_logs = None;
                log_entry
            })
            .collect::<Vec<_>>()
    });

    task
}
