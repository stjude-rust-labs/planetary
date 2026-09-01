// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "task_state"))]
    pub struct TaskState;
}

diesel::table! {
    containers (id) {
        id -> Int4,
        task_id -> Int4,
        name -> Text,
        executor_index -> Nullable<Int4>,
        start_time -> Timestamptz,
        end_time -> Timestamptz,
        stdout -> Nullable<Text>,
        stderr -> Nullable<Text>,
        exit_code -> Int4,
        creation_time -> Timestamptz,
    }
}

diesel::table! {
    errors (id) {
        id -> Int4,
        source -> Text,
        task_id -> Nullable<Int4>,
        message -> Text,
        creation_time -> Timestamptz,
    }
}

diesel::table! {
    task_container_baseline (task_id, pod_name, container_name) {
        task_id -> Int4,
        pod_name -> Text,
        container_name -> Text,
        start_time_seconds -> Nullable<Float8>,
        cpu_seconds -> Float8,
    }
}

diesel::table! {
    task_container_usage (task_id, container_name) {
        task_id -> Int4,
        container_name -> Text,
        peak_memory_bytes -> Nullable<Int8>,
        memory_total_bytes -> Nullable<Int8>,
        memory_sample_count -> Nullable<Int8>,
        cpu_time_ms -> Nullable<Int8>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::TaskState;

    tasks (id) {
        id -> Int4,
        username -> Text,
        tes_id -> Text,
        state -> TaskState,
        name -> Nullable<Text>,
        description -> Nullable<Text>,
        inputs -> Nullable<Jsonb>,
        outputs -> Nullable<Jsonb>,
        cpu_cores -> Nullable<Int4>,
        preemptible -> Nullable<Bool>,
        ram_gb -> Nullable<Float8>,
        disk_gb -> Nullable<Float8>,
        zones -> Nullable<Array<Nullable<Text>>>,
        backend_parameters -> Nullable<Jsonb>,
        backend_parameters_strict -> Nullable<Bool>,
        executors -> Jsonb,
        volumes -> Nullable<Array<Nullable<Text>>>,
        tags -> Nullable<Jsonb>,
        output_files -> Nullable<Jsonb>,
        system_logs -> Nullable<Array<Nullable<Text>>>,
        creation_time -> Timestamptz,
    }
}

diesel::joinable!(containers -> tasks (task_id));
diesel::joinable!(errors -> tasks (task_id));
diesel::joinable!(task_container_baseline -> tasks (task_id));
diesel::joinable!(task_container_usage -> tasks (task_id));

diesel::allow_tables_to_appear_in_same_query!(
    containers,
    errors,
    task_container_baseline,
    task_container_usage,
    tasks,
);
