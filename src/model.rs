use serde::{Deserialize, Serialize};

/// In-memory representation of the tracking data.
///
/// The format is intentionally compatible with both a simple JSON layout and the
/// "db-style" layout used by the original TS scripts (possessions/possessions_containers/possessions_usage).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SubstanceData {
    #[serde(default)]
    pub acquisition: Vec<Acquisition>,
    #[serde(default)]
    pub containers: Vec<Container>,
    #[serde(default)]
    pub usage_log: Vec<UsageEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Acquisition {
    #[serde(rename = "acquire_date")]
    pub acquire_date: Option<String>,
    #[serde(rename = "value_g")]
    pub value_g: f64,
    pub unit: Option<String>,
    pub cost: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Container {
    pub id: String,
    pub label: Option<String>,
    pub tare_weight_g: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UsageEntry {
    #[serde(rename = "type")]
    pub r#type: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub timestamp: Option<String>,
    pub amount: f64,
    pub amount_unit: Option<String>,
}
