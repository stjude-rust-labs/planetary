//! Provides an implementation of rendering a task resource template.
//!
//! This is used by both the orchestrator when creating task resources and by
//! the monitor when tasks are garbage collected.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

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
use reqwest::Url;
use serde::Deserialize as _;
use serde_yaml_ng::Deserializer;
use tera::Context;
use tera::Map;
use tera::Tera;
use tera::Value;
use tera_contrib::json::json_encode;
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

/// Helper for converting a URL to a file path.
///
/// Returns `None` if the URL is invalid or if the URL does not represent a file
/// path.
fn url_to_file_path(url: &str) -> Option<PathBuf> {
    let url = Url::parse(url).ok()?;
    if url.scheme() != "file" {
        return None;
    }

    url.to_file_path().ok()
}

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

        let mut templates = Tera::new();
        templates.register_filter("json_encode", json_encode);

        templates.load_from_glob(templates_dir.join("**/*").to_str().with_context(|| {
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
                username: String::new(),
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
        let mut context = Context::new();
        context.insert("id", data.id.as_str());
        context.insert("username", data.username.as_str());
        context.insert("preemptible", &data.preemptible);

        // Set the requested resources
        context.insert("cpu", &data.cpu.unwrap_or(DEFAULT_CPU));
        context.insert(
            "memory",
            &format!(
                "{memory}G",
                memory = data.memory.unwrap_or(DEFAULT_MEMORY).ceil()
            ),
        );
        context.insert(
            "disk",
            &format!(
                "{disk}G",
                disk = data.disk.unwrap_or(0.0).max(DEFAULT_STORAGE_SIZE)
            ),
        );

        // Set the inputs
        let inputs: Vec<_> = data
            .inputs
            .iter()
            .map(|input| {
                let mut value: HashMap<_, Cow<'_, str>> = HashMap::new();
                value.insert("path", input.path.as_str().into());

                if let Some(local_path) = input.url.as_ref().and_then(|url| {
                    Some(
                        // SAFETY: URLs are validated when tasks are created
                        url_to_file_path(url)?
                            .strip_prefix("/")
                            .expect("path should be absolute")
                            .to_str()
                            .expect("path should be UTF-8")
                            .to_string(),
                    )
                }) {
                    value.insert("local_path", local_path.into());
                }

                value
            })
            .collect();
        context.insert("inputs", &inputs);

        // Set the outputs
        let outputs: Vec<_> = data
            .outputs
            .iter()
            .map(|output| {
                let mut value: HashMap<_, Cow<'_, str>> = HashMap::new();
                value.insert(
                    "path",
                    output.path_prefix.as_deref().unwrap_or(&output.path).into(),
                );

                if let Some(path) = url_to_file_path(&output.url) {
                    // SAFETY: URLs are validated when tasks are created
                    let local_path = path
                        .strip_prefix("/")
                        .expect("path should be absolute")
                        .to_str()
                        .expect("path should be UTF-8");
                    value.insert("local_path", local_path.to_string().into());
                }

                value
            })
            .collect();
        context.insert("outputs", &outputs);

        // Set the volumes
        context.insert("volumes", &data.volumes);

        // Set the executors
        let executors: Vec<_> = data
            .executors
            .iter()
            .map(|e| {
                let mut value: HashMap<_, Value> = HashMap::new();
                value.insert("image", e.image.as_str().into());
                value.insert("script", script(e)?.into());
                value.insert("workdir", e.workdir.as_deref().unwrap_or_default().into());

                if let Some(env) = e.env.as_ref() {
                    value.insert("env", Value::from_serializable(env));
                } else {
                    value.insert("env", Map::new().into());
                }

                Ok(value)
            })
            .collect::<Result<_>>()?;
        context.insert("executors", &executors);

        Ok(context)
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
