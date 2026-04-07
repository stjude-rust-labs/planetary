// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "container_kind"))]
    pub struct ContainerKind;

    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "task_state"))]
    pub struct TaskState;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::ContainerKind;

    containers (id) {
        id -> Int4,
        task_id -> Int4,
        kind -> ContainerKind,
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

diesel::joinable!(containers -> tasks (task_id));
diesel::joinable!(errors -> tasks (task_id));

diesel::allow_tables_to_appear_in_same_query!(containers, errors, tasks,);
