//! Functional tests for Planetary.
//!
//! These tests deploy Planetary into a `kind` cluster and exercise the TES
//! API; see the crate documentation of `planetary_tests` for the
//! requirements and supported environment variables.
//!
//! As the tests require external tools, they are ignored by default; run
//! them with:
//!
//! ```sh
//! cargo test -p planetary-tests -- --ignored
//! ```

use planetary_tests::TestEnvironment;
use planetary_tests::take;
use planetary_tests::unique_username;
use serde_json::Value;
use serde_json::json;

/// The container image used for task executors.
const EXECUTOR_IMAGE: &str = "alpine:latest";

/// Creates a task request with a single executor running the given shell
/// command.
fn shell_task(command: &str) -> Value {
    json!({
        "executors": [
            {
                "image": EXECUTOR_IMAGE,
                "command": ["sh", "-c", command],
            }
        ]
    })
}

/// Tests that the service information is returned as expected.
///
/// The service information route does not require authentication.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn service_info() {
    let env = TestEnvironment::get();
    let client = env.client(None);

    let (status, body) = client.get("/v1/service-info");
    assert_eq!(status, 200, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "id": "org.example.planetary",
            "name": "Planetary Server",
            "type": {
                "group": "org.ga4gh",
                "artifact": "tes",
                "version": "1.1.0",
            },
            "organization": {
                "name": "Example Organization",
                "url": "https://example.com/",
            },
            "version": "0.1.0",
        })
    );
}

/// Tests that task routes are forbidden without a username.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn forbidden_without_username() {
    let env = TestEnvironment::get();
    let client = env.client(None);

    let (status, body) = client.get("/v1/tasks");
    assert_eq!(status, 403, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "status": 403,
            "message": "403 Forbidden",
        })
    );
}

/// Tests that the `Authorization` header is used as a fallback for the
/// username when the `X-Forwarded-User` header is not present.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn basic_auth_fallback() {
    let env = TestEnvironment::get();
    let client = env.basic_auth_client(&unique_username("basic-auth"));

    let (status, body) = client.get("/v1/tasks");
    assert_eq!(status, 200, "unexpected response: {body:#}");
    assert_eq!(body, json!({ "tasks": [] }));
}

/// Tests that an unknown route responds with a "not found" error.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn unknown_route() {
    let env = TestEnvironment::get();
    let client = env.client(None);

    let (status, body) = client.get("/v1/does-not-exist");
    assert_eq!(status, 404, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "status": 404,
            "message": "the requested resource was not found",
        })
    );
}

/// Tests that getting an unknown task responds with a "not found" error.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn unknown_task() {
    let env = TestEnvironment::get();
    let client = env.client(Some(&unique_username("unknown-task")));

    let (status, body) = client.get("/v1/tasks/00000000000000000000");
    assert_eq!(status, 404, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "status": 404,
            "message": "task `00000000000000000000` was not found",
        })
    );
}

/// Tests that invalid task requests are rejected.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn invalid_tasks() {
    let env = TestEnvironment::get();
    let client = env.client(Some(&unique_username("invalid-tasks")));

    // A task must have at least one executor
    let (status, body) = client.post("/v1/tasks", &json!({ "executors": [] }));
    assert_eq!(status, 400, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "status": 400,
            "message": "task must contain at least one executor",
        })
    );

    // An input URL must use a supported scheme
    let mut task = shell_task("true");
    task["inputs"] = json!([
        {
            "url": "ftp://example.com/input.txt",
            "path": "/data/input.txt",
            "type": "FILE",
        }
    ]);
    let (status, body) = client.post("/v1/tasks", &task);
    assert_eq!(status, 400, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "status": 400,
            "message": "input URL `ftp://example.com/input.txt` uses unsupported scheme `ftp`",
        })
    );

    // An output path must be absolute
    let mut task = shell_task("true");
    task["outputs"] = json!([
        {
            "url": "https://example.com/output.txt",
            "path": "data/output.txt",
            "type": "FILE",
        }
    ]);
    let (status, body) = client.post("/v1/tasks", &task);
    assert_eq!(status, 400, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "status": 400,
            "message": "output path `data/output.txt` is not absolute",
        })
    );
}

/// Tests the lifecycle of a task that runs to completion, including the
/// contents of the `MINIMAL`, `BASIC`, and `FULL` task views.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn task_lifecycle() {
    let env = TestEnvironment::get();
    let client = env.client(Some(&unique_username("lifecycle")));

    let id = client.create_task(&json!({
        "name": "hello-world",
        "executors": [
            {
                "image": EXECUTOR_IMAGE,
                "command": ["echo", "hello, world!"],
            }
        ]
    }));

    client.wait_for_task_state(&id, "COMPLETE");

    // The minimal view contains only the id and state
    let (status, body) = client.get_task(&id, "MINIMAL");
    assert_eq!(status, 200, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "id": id,
            "state": "COMPLETE",
        })
    );

    // The basic view echoes the task request but strips executor output
    let (status, mut body) = client.get_task(&id, "BASIC");
    assert_eq!(status, 200, "unexpected response: {body:#}");

    let creation_time = take(&mut body, "/creation_time");
    assert!(
        creation_time.is_string(),
        "unexpected creation time: {creation_time:#}"
    );

    let logs = take(&mut body, "/logs");
    let executor_logs = logs[0]["logs"]
        .as_array()
        .expect("executor logs should be an array");
    assert_eq!(executor_logs.len(), 1, "unexpected logs: {logs:#}");
    assert_eq!(
        executor_logs[0]["exit_code"], 0,
        "unexpected logs: {logs:#}"
    );
    assert!(
        executor_logs[0].get("stdout").is_none(),
        "the basic view should not contain executor stdout: {logs:#}"
    );

    assert_eq!(
        body,
        json!({
            "id": id,
            "state": "COMPLETE",
            "name": "hello-world",
            "executors": [
                {
                    "image": EXECUTOR_IMAGE,
                    "command": ["echo", "hello, world!"],
                }
            ],
        })
    );

    // The full view also contains the executor output
    let (status, body) = client.get_task(&id, "FULL");
    assert_eq!(status, 200, "unexpected response: {body:#}");
    assert_eq!(
        body["logs"][0]["logs"][0]["stdout"], "hello, world!\n",
        "unexpected response: {body:#}"
    );
    assert_eq!(
        body["logs"][0]["logs"][0]["exit_code"], 0,
        "unexpected response: {body:#}"
    );
}

/// Tests that tasks are only visible to the user that created them.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn username_isolation() {
    let env = TestEnvironment::get();
    let first = env.client(Some(&unique_username("isolation-first")));
    let second = env.client(Some(&unique_username("isolation-second")));

    let id = first.create_task(&shell_task("true"));

    // The task should not be visible to the second user
    let (status, body) = second.get_task(&id, "MINIMAL");
    assert_eq!(status, 404, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "status": 404,
            "message": format!("task `{id}` was not found"),
        })
    );

    let (status, body) = second.get("/v1/tasks");
    assert_eq!(status, 200, "unexpected response: {body:#}");
    assert_eq!(body, json!({ "tasks": [] }));
}

/// Tests listing tasks with a name prefix filter and paginating the results.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn list_tasks() {
    /// Gets the set of task ids in a list response.
    fn ids(body: &Value) -> Vec<&str> {
        body["tasks"]
            .as_array()
            .expect("tasks should be an array")
            .iter()
            .map(|t| t["id"].as_str().expect("task id should be a string"))
            .collect()
    }

    let env = TestEnvironment::get();
    let client = env.client(Some(&unique_username("list-tasks")));

    let mut first_task = shell_task("true");
    first_task["name"] = json!("list-tasks-first");
    let first = client.create_task(&first_task);

    let mut second_task = shell_task("true");
    second_task["name"] = json!("list-tasks-second");
    let second = client.create_task(&second_task);

    // Listing with a name prefix should return only the matching task
    let (status, body) = client.get("/v1/tasks?view=MINIMAL&name_prefix=list-tasks-first");
    assert_eq!(status, 200, "unexpected response: {body:#}");
    assert_eq!(
        ids(&body),
        [first.as_str()],
        "unexpected response: {body:#}"
    );

    // Paginating with a page size of one should return both tasks across two
    // pages
    let (status, body) = client.get("/v1/tasks?view=MINIMAL&page_size=1");
    assert_eq!(status, 200, "unexpected response: {body:#}");
    let first_page = ids(&body);
    assert_eq!(first_page.len(), 1, "unexpected response: {body:#}");

    let token = body["next_page_token"]
        .as_str()
        .expect("response should have a next page token");
    let (status, next) = client.get(&format!(
        "/v1/tasks?view=MINIMAL&page_size=1&page_token={token}"
    ));
    assert_eq!(status, 200, "unexpected response: {next:#}");
    let second_page = ids(&next);
    assert_eq!(second_page.len(), 1, "unexpected response: {next:#}");

    let mut all = vec![first_page[0], second_page[0]];
    all.sort();
    let mut expected = vec![first.as_str(), second.as_str()];
    expected.sort();
    assert_eq!(all, expected);
}

/// Tests canceling a running task.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn cancel_task() {
    let env = TestEnvironment::get();
    let client = env.client(Some(&unique_username("cancel")));

    let id = client.create_task(&shell_task("sleep 300"));

    // Cancellation responds with an empty object
    let (status, body) = client.cancel_task(&id);
    assert_eq!(status, 200, "unexpected response: {body:#}");
    assert_eq!(body, json!({}));

    client.wait_for_task_state(&id, "CANCELED");

    // A canceled task cannot be canceled again
    let (status, body) = client.cancel_task(&id);
    assert_eq!(status, 400, "unexpected response: {body:#}");
    assert_eq!(
        body,
        json!({
            "status": 400,
            "message": "task is not in a cancelable state",
        })
    );
}

/// Tests a task that reads its input from and writes its output to "local"
/// (`file://`) storage.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn local_storage_roundtrip() {
    /// The content of the local input file.
    const CONTENT: &str = "hello from local storage\n";

    let env = TestEnvironment::get();
    let username = unique_username("local-storage");
    let client = env.client(Some(&username));

    env.write_local_file(&username, "inputs/greeting.txt", CONTENT);

    let id = client.create_task(&json!({
        "inputs": [
            {
                "url": "file:///inputs/greeting.txt",
                "path": "/data/input.txt",
                "type": "FILE",
            }
        ],
        "outputs": [
            {
                "url": "file:///outputs/copy.txt",
                "path": "/data/output.txt",
                "type": "FILE",
            }
        ],
        "executors": [
            {
                "image": EXECUTOR_IMAGE,
                "command": ["sh", "-c", "cat /data/input.txt > /data/output.txt"],
            }
        ]
    }));

    client.wait_for_task_state(&id, "COMPLETE");

    assert_eq!(env.read_local_file(&username, "outputs/copy.txt"), CONTENT);
}

/// Tests a task with a wildcard output: a `FILE` output whose `path` is a
/// glob pattern and whose `path_prefix` is the directory to scan.
///
/// Only the files matching the pattern should be recorded as output files in
/// the task log.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn file_wildcard_outputs() {
    let env = TestEnvironment::get();
    let username = unique_username("file-wildcard-outputs");
    let client = env.client(Some(&username));

    let id = client.create_task(&json!({
        "outputs": [
            {
                "url": "file:///outputs/wildcard",
                "path": "/data/out/*.txt",
                "path_prefix": "/data/out",
                "type": "FILE",
            }
        ],
        "executors": [
            {
                "image": EXECUTOR_IMAGE,
                "command": [
                    "sh",
                    "-c",
                    "echo first > /data/out/first.txt && \
                     echo second > /data/out/second.txt && \
                     echo excluded > /data/out/excluded.log && \
                     mkdir /data/out/nested && \
                     echo nested > /data/out/nested/excluded.txt",
                ],
            }
        ]
    }));

    client.wait_for_task_state(&id, "COMPLETE");

    // The files matching the pattern should be present in local storage
    assert_eq!(
        env.read_local_file(&username, "outputs/wildcard/first.txt"),
        "first\n"
    );
    assert_eq!(
        env.read_local_file(&username, "outputs/wildcard/second.txt"),
        "second\n"
    );

    // Only the files matching the pattern should be recorded as output files
    let (status, body) = client.get_task(&id, "FULL");
    assert_eq!(status, 200, "unexpected response: {body:#}");

    let mut outputs = body["logs"][0]["outputs"]
        .as_array()
        .expect("outputs should be an array")
        .clone();
    outputs.sort_by_key(|o| {
        o["path"]
            .as_str()
            .expect("output path should be a string")
            .to_string()
    });
    assert_eq!(
        outputs,
        [
            json!({
                "url": "file:///outputs/wildcard/first.txt",
                "path": "/data/out/first.txt",
                "size_bytes": "6",
            }),
            json!({
                "url": "file:///outputs/wildcard/second.txt",
                "path": "/data/out/second.txt",
                "size_bytes": "7",
            }),
        ],
        "unexpected response: {body:#}"
    );
}

/// Tests a task with a `DIRECTORY` output.
///
/// Every file in the directory, including files in nested directories, should
/// be recorded as an output file in the task log.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn directory_outputs() {
    let env = TestEnvironment::get();
    let username = unique_username("directory-outputs");
    let client = env.client(Some(&username));

    let id = client.create_task(&json!({
        "outputs": [
            {
                "url": "file:///outputs/dir",
                "path": "/data/out",
                "type": "DIRECTORY",
            }
        ],
        "executors": [
            {
                "image": EXECUTOR_IMAGE,
                "command": [
                    "sh",
                    "-c",
                    "echo first > /data/out/first.txt && \
                     echo second > /data/out/second.txt && \
                     mkdir /data/out/nested && \
                     echo third > /data/out/nested/third.txt",
                ],
            }
        ]
    }));

    client.wait_for_task_state(&id, "COMPLETE");

    // The directory contents should be present in local storage
    assert_eq!(
        env.read_local_file(&username, "outputs/dir/first.txt"),
        "first\n"
    );
    assert_eq!(
        env.read_local_file(&username, "outputs/dir/second.txt"),
        "second\n"
    );
    assert_eq!(
        env.read_local_file(&username, "outputs/dir/nested/third.txt"),
        "third\n"
    );

    // Every file in the directory should be recorded as an output file
    let (status, body) = client.get_task(&id, "FULL");
    assert_eq!(status, 200, "unexpected response: {body:#}");

    let mut outputs = body["logs"][0]["outputs"]
        .as_array()
        .expect("outputs should be an array")
        .clone();
    outputs.sort_by_key(|o| {
        o["path"]
            .as_str()
            .expect("output path should be a string")
            .to_string()
    });
    assert_eq!(
        outputs,
        [
            json!({
                "url": "file:///outputs/dir/first.txt",
                "path": "/data/out/first.txt",
                "size_bytes": "6",
            }),
            json!({
                "url": "file:///outputs/dir/nested/third.txt",
                "path": "/data/out/nested/third.txt",
                "size_bytes": "6",
            }),
            json!({
                "url": "file:///outputs/dir/second.txt",
                "path": "/data/out/second.txt",
                "size_bytes": "7",
            }),
        ],
        "unexpected response: {body:#}"
    );
}

/// Tests a task with a wildcard `DIRECTORY` output: a `DIRECTORY` output
/// whose `path` is a glob pattern and whose `path_prefix` is the directory to
/// scan.
///
/// Only the files matching the pattern, including files in nested
/// directories, should be recorded as output files in the task log.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn directory_wildcard_outputs() {
    let env = TestEnvironment::get();
    let username = unique_username("directory-wildcard-outputs");
    let client = env.client(Some(&username));

    let id = client.create_task(&json!({
        "outputs": [
            {
                "url": "file:///outputs/dir-wildcard",
                "path": "/data/out/**/*.txt",
                "path_prefix": "/data/out",
                "type": "DIRECTORY",
            }
        ],
        "executors": [
            {
                "image": EXECUTOR_IMAGE,
                "command": [
                    "sh",
                    "-c",
                    "echo first > /data/out/first.txt && \
                     echo second > /data/out/second.txt && \
                     mkdir /data/out/nested && \
                     echo third > /data/out/nested/third.txt && \
                     echo excluded > /data/out/excluded.log",
                ],
            }
        ]
    }));

    client.wait_for_task_state(&id, "COMPLETE");

    // The files matching the pattern should be present in local storage
    assert_eq!(
        env.read_local_file(&username, "outputs/dir-wildcard/first.txt"),
        "first\n"
    );
    assert_eq!(
        env.read_local_file(&username, "outputs/dir-wildcard/second.txt"),
        "second\n"
    );
    assert_eq!(
        env.read_local_file(&username, "outputs/dir-wildcard/nested/third.txt"),
        "third\n"
    );

    // Only the files matching the pattern should be recorded as output files
    let (status, body) = client.get_task(&id, "FULL");
    assert_eq!(status, 200, "unexpected response: {body:#}");

    let mut outputs = body["logs"][0]["outputs"]
        .as_array()
        .expect("outputs should be an array")
        .clone();
    outputs.sort_by_key(|o| {
        o["path"]
            .as_str()
            .expect("output path should be a string")
            .to_string()
    });
    assert_eq!(
        outputs,
        [
            json!({
                "url": "file:///outputs/dir-wildcard/first.txt",
                "path": "/data/out/first.txt",
                "size_bytes": "6",
            }),
            json!({
                "url": "file:///outputs/dir-wildcard/nested/third.txt",
                "path": "/data/out/nested/third.txt",
                "size_bytes": "6",
            }),
            json!({
                "url": "file:///outputs/dir-wildcard/second.txt",
                "path": "/data/out/second.txt",
                "size_bytes": "7",
            }),
        ],
        "unexpected response: {body:#}"
    );
}

/// Tests a task with an input specified by embedded content rather than a
/// URL.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn input_content() {
    /// The content of the input file.
    const CONTENT: &str = "embedded input content\n";

    let env = TestEnvironment::get();
    let client = env.client(Some(&unique_username("input-content")));

    let id = client.create_task(&json!({
        "inputs": [
            {
                "content": CONTENT,
                "path": "/data/input.txt",
                "type": "FILE",
            }
        ],
        "executors": [
            {
                "image": EXECUTOR_IMAGE,
                "command": ["cat", "/data/input.txt"],
            }
        ]
    }));

    client.wait_for_task_state(&id, "COMPLETE");

    let (status, body) = client.get_task(&id, "FULL");
    assert_eq!(status, 200, "unexpected response: {body:#}");
    assert_eq!(
        body["logs"][0]["logs"][0]["stdout"], CONTENT,
        "unexpected response: {body:#}"
    );
}

/// Tests that a task's executors run sequentially and share the task's
/// `/tmp` volume.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn sequential_executors() {
    let env = TestEnvironment::get();
    let client = env.client(Some(&unique_username("sequential")));

    let id = client.create_task(&json!({
        "executors": [
            {
                "image": EXECUTOR_IMAGE,
                "command": ["sh", "-c", "echo from the first executor > /tmp/shared.txt"],
            },
            {
                "image": EXECUTOR_IMAGE,
                "command": ["cat", "/tmp/shared.txt"],
            }
        ]
    }));

    client.wait_for_task_state(&id, "COMPLETE");

    let (status, body) = client.get_task(&id, "FULL");
    assert_eq!(status, 200, "unexpected response: {body:#}");

    let executor_logs = body["logs"][0]["logs"]
        .as_array()
        .expect("executor logs should be an array");
    assert_eq!(executor_logs.len(), 2, "unexpected response: {body:#}");
    assert_eq!(
        executor_logs[0]["exit_code"], 0,
        "unexpected response: {body:#}"
    );
    assert_eq!(
        executor_logs[1]["exit_code"], 0,
        "unexpected response: {body:#}"
    );
    assert_eq!(
        executor_logs[1]["stdout"], "from the first executor\n",
        "unexpected response: {body:#}"
    );
}

/// Tests that a failing executor results in an executor error with the
/// executor's exit code and error output.
#[test]
#[ignore = "requires `docker`, `kind`, `kubectl`, and `helm`"]
fn executor_failure() {
    let env = TestEnvironment::get();
    let client = env.client(Some(&unique_username("failure")));

    let id = client.create_task(&shell_task("echo something went wrong >&2; exit 3"));

    client.wait_for_task_state(&id, "EXECUTOR_ERROR");

    let (status, body) = client.get_task(&id, "FULL");
    assert_eq!(status, 200, "unexpected response: {body:#}");
    assert_eq!(
        body["logs"][0]["logs"][0]["exit_code"], 3,
        "unexpected response: {body:#}"
    );
    assert_eq!(
        body["logs"][0]["logs"][0]["stderr"], "something went wrong\n",
        "unexpected response: {body:#}"
    );
}
