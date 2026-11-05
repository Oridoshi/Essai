use serde::{Serialize, Deserialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum WorkerStatus{
    Available,
    Full,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum WorkerState{
    WarmUp,
    Idle,
    Running,
    Stopping,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub struct Heartbeat {
    pub id: Uuid,

    pub cpu_usage: u32, // car *100 pour éviter les float
    pub ram_usage: u32, // car *100 pour éviter les float

    pub tasks_count: u32,
    pub max_tasks: u32,
    //pub exec_time_ms_tab: Vec<u64>,

    pub status: WorkerStatus,
    pub state: WorkerState,
}