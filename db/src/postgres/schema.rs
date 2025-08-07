// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "pod_kind"))]
    pub struct PodKind;

    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "pod_state"))]
    pub struct PodState;

    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "task_state"))]
    pub struct TaskState;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::PodKind;
    use super::sql_types::PodState;

    pods (id) {
        id -> Int4,
        task_id -> Int4,
        name -> Text,
        kind -> PodKind,
        state -> PodState,
        executor_index -> Nullable<Int4>,
        start_time -> Nullable<Timestamptz>,
        end_time -> Nullable<Timestamptz>,
        stdout -> Nullable<Text>,
        stderr -> Nullable<Text>,
        exit_code -> Nullable<Int4>,
        creation_time -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::TaskState;

    tasks (id) {
        id -> Int4,
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

diesel::joinable!(pods -> tasks (task_id));

diesel::allow_tables_to_appear_in_same_query!(pods, tasks,);
