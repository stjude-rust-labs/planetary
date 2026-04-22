//! Provides an implementation of rendering a task resource template.
//!
//! This is used by both the orchestrator when creating task resources and by
//! the monitor when tasks are garbage collected.

use std::path::Path;

use anyhow::Context as _;
use anyhow::Result;
use anyhow::bail;
use kube::Discovery;
use kube::api::ApiResource;
use kube::api::DynamicObject;
use kube::api::GroupVersionKind;
use kube::discovery::ApiCapabilities;
use kube::discovery::Scope;
use kube::runtime::reflector::Lookup;
use planetary_db::TaskTemplateData;
use serde::Deserialize as _;
use serde_yaml_ng::Deserializer;
use tera::Context;
use tera::Map;
use tera::Tera;
use tera::Value;
use tes::v1::types::task::Executor;

/// The orchestrator id label.
pub const ORCHESTRATOR_LABEL: &str = "planetary/orchestrator";

/// The task id label.
pub const TASK_LABEL: &str = "planetary/task";

/// The cancellation label used to mark canceled task for garbage collection.
pub const CANCELED_LABEL: &str = "planetary/canceled";

/// The expected name of the task resource template.
const TEMPLATE_NAME: &str = "task.yaml";

/// The default task storage size, in gigabytes.
///
/// Uses a 1 GiB default.
const DEFAULT_STORAGE_SIZE: f64 = 1.07374182;

/// The default CPU request (in cores) for tasks.
const DEFAULT_CPU: i32 = 1;

/// The default memory request (in GB) for tasks.
///
/// Uses a 256 MiB default.
const DEFAULT_MEMORY: f64 = 0.268435455;

/// Represents a Planetary task template.
///
/// A task template defines the Kubernetes resources that will be created for
/// each TES task.
pub struct Template(Tera);

impl Template {
    /// Constructs a new Planetary task template.
    ///
    /// The specified directory must contain a `task.yaml` template.
    pub fn new(templates_dir: impl AsRef<Path>) -> Result<Self> {
        let templates_dir = templates_dir.as_ref();

        let templates = Tera::new(templates_dir.join("**/*").to_str().with_context(|| {
            format!(
                "templates directory `{path}` is not valid UTF-8",
                path = templates_dir.display()
            )
        })?)?;

        if !templates.get_template_names().any(|n| n == TEMPLATE_NAME) {
            bail!(
                "templates directory `{path}` does not contain a template named `{TEMPLATE_NAME}`",
                path = templates_dir.display()
            );
        }

        Ok(Self(templates))
    }

    /// Renders a template for the given TES task.
    ///
    /// The TES task must have at least the task id present.
    ///
    /// The provided discovery is used to validate the requested Kubernetes
    /// resources.
    ///
    /// The provided Kubernetes namespace is used to apply to the task
    /// resources.
    ///
    /// The provided script callback is called once per executor defined on the
    /// task to retrieve the executor's script.
    ///
    /// Returns the set of resources defined by the task template.
    ///
    /// An error is returned if the template does not define exactly one pod
    /// with a restart policy of `Never`.
    pub fn render(
        &self,
        data: &TaskTemplateData,
        discovery: &Discovery,
        namespace: &str,
        script: impl Fn(&Executor) -> Result<String>,
    ) -> Result<Vec<TaskResource>> {
        let rendered = self
            .0
            .render(TEMPLATE_NAME, &Self::create_context(data, script)?)
            .context("failed to render task resource template")?;

        let resources = serde_yaml_ng::Deserializer::from_str(&rendered)
            .map(|de| self.deserialize_object(&data.id, discovery, namespace, de))
            .collect::<Result<Vec<_>>>()?;

        // Ensure there is exactly one pod that has a restart policy of `Never`
        let mut has_pod = false;

        for r in &resources {
            if r.api().api_version == "v1" && r.api().kind == "Pod" {
                if has_pod {
                    bail!("task template defines more than one pod for the task")
                }

                if r.object()
                    .data
                    .get("spec")
                    .and_then(|o| o.get("restartPolicy"))
                    .map(|v| v.as_str() != Some("Never"))
                    .unwrap_or(true)
                {
                    bail!("task pod must have a `Never` restart policy");
                }

                has_pod = true;
            }
        }

        if !has_pod {
            bail!("task template does not define a pod for the task");
        }

        Ok(resources)
    }

    /// Renders the template with only the given TES task identifier.
    pub fn render_id_only(
        &self,
        id: impl Into<String>,
        discovery: &Discovery,
        namespace: &str,
    ) -> Result<Vec<TaskResource>> {
        // Render the template using only the identifier of the task
        self.render(
            &TaskTemplateData {
                id: id.into(),
                preemptible: false,
                cpu: None,
                memory: None,
                disk: None,
                inputs: Default::default(),
                outputs: Default::default(),
                volumes: Default::default(),
                executors: Default::default(),
            },
            discovery,
            namespace,
            |_| Ok(String::new()),
        )
    }

    /// Creates a template context for a TES task.
    ///
    /// The provided callback is used to format an executor script for the
    /// template.
    ///
    /// Returns an error if the task was invalid.
    fn create_context(
        data: &TaskTemplateData,
        script: impl Fn(&Executor) -> Result<String>,
    ) -> Result<Context> {
        /// Helper for inserting items into the context.
        fn insert(
            context: &mut Map<String, Value>,
            name: impl Into<String>,
            value: impl Into<Value>,
        ) {
            context.insert(name.into(), value.into());
        }

        let mut context = Map::new();
        insert(&mut context, "id", data.id.as_str());
        insert(&mut context, "preemptible", data.preemptible);

        // Set the requested resources
        insert(&mut context, "cpu", data.cpu.unwrap_or(DEFAULT_CPU));
        insert(
            &mut context,
            "memory",
            format!(
                "{memory}G",
                memory = data.memory.unwrap_or(DEFAULT_MEMORY).ceil()
            ),
        );
        insert(
            &mut context,
            "disk",
            format!(
                "{disk}G",
                disk = data.disk.unwrap_or(0.0).max(DEFAULT_STORAGE_SIZE)
            ),
        );

        // Set the inputs
        let inputs: Vec<Value> = data.inputs.iter().map(|i| i.path.clone().into()).collect();
        insert(&mut context, "inputs", inputs);

        // Set the outputs
        let outputs: Vec<Value> = data.outputs.iter().map(|o| o.path.clone().into()).collect();
        insert(&mut context, "outputs", outputs);

        // Set the volumes
        insert(&mut context, "volumes", data.volumes.as_slice());

        // Set the executors
        let executors: Vec<Value> = data
            .executors
            .iter()
            .map(|e| {
                let mut executor = Map::new();
                insert(&mut executor, "image", e.image.clone());
                insert(&mut executor, "script", script(e)?);
                insert(
                    &mut executor,
                    "workdir",
                    e.workdir.as_deref().unwrap_or_default(),
                );

                let mut env = Map::new();
                if let Some(vars) = e.env.as_ref() {
                    for (k, v) in vars {
                        insert(&mut env, k, v.clone());
                    }
                }

                insert(&mut executor, "env", env);
                Ok(executor.into())
            })
            .collect::<Result<_>>()?;
        insert(&mut context, "executors", executors);

        // Construct the context directly from the map as a JSON object
        Context::from_value(context.into()).context("invalid template context")
    }

    /// Deserializes a Kubernetes object and returns its resolved API resources
    /// and capabilities.
    fn deserialize_object(
        &self,
        tes_id: &str,
        discovery: &Discovery,
        namespace: &str,
        de: Deserializer<'_>,
    ) -> Result<TaskResource> {
        let mut object = DynamicObject::deserialize(de)
            .context("failed to deserialize task resource template")?;

        let name = object
            .name()
            .context("task template contains a resource that has no name")?;

        let meta = object.types.as_ref().with_context(|| {
            format!("task resource `{name}` does not specify an object API version and kind")
        })?;

        let gvk = GroupVersionKind::try_from(meta).with_context(|| {
            format!(
                "task resource `{name}` has invalid kind: `{kind}` ({api})",
                kind = meta.kind,
                api = meta.api_version
            )
        })?;

        // Set the task label for the object
        let labels = object.metadata.labels.get_or_insert_default();
        labels.insert(TASK_LABEL.to_string(), tes_id.to_string());

        let (resource, capabilities) = discovery.resolve_gvk(&gvk).with_context(|| {
            format!(
                "task resource `{name}` has unknown resource kind `{kind}` ({api})",
                name = object.name().expect("object should have a name"),
                kind = meta.kind,
                api = meta.api_version
            )
        })?;

        if capabilities.scope == Scope::Cluster {
            object.metadata.namespace = None;
        } else {
            object.metadata.namespace = Some(namespace.to_string());
        }

        Ok(TaskResource {
            resource,
            capabilities,
            object,
        })
    }
}

/// Represents a requested task resource from a task template.
pub struct TaskResource {
    /// The Kubernetes API for the resource.
    resource: ApiResource,
    /// The capabilities of the resource's API.
    capabilities: ApiCapabilities,
    /// The object defining the resource.
    object: DynamicObject,
}

impl TaskResource {
    /// Gets the Kubernetes API resource.
    pub fn api(&self) -> &ApiResource {
        &self.resource
    }

    /// Gets the Kubernetes API capabilities.
    pub fn capabilities(&self) -> &ApiCapabilities {
        &self.capabilities
    }

    /// Gets the object defining the resource.
    pub fn object(&self) -> &DynamicObject {
        &self.object
    }

    /// Gets a mutable reference to the object defining the resource.
    pub fn object_mut(&mut self) -> &mut DynamicObject {
        &mut self.object
    }
}
