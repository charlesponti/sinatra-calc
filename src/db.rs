use std::path::Path;

use anyhow::Result;
use chrono::Utc;
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};

use crate::model::{Acquisition, Container, SubstanceData, UsageEntry};

pub const DEFAULT_DATABASE_PATH: &str = "mater.db";

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, sqlx::FromRow)]
struct AcquisitionRow {
    acquired_date: Option<String>,
    amount: Option<f64>,
    amount_unit: Option<String>,
    price: Option<f64>,
    purchase_price: Option<f64>,
}

#[derive(Debug, sqlx::FromRow)]
struct ContainerRow {
    id: String,
    label: Option<String>,
    tare_weight_g: Option<f64>,
}

#[derive(Debug, sqlx::FromRow)]
struct UsageRow {
    entry_type: String,
    start_date: Option<String>,
    end_date: Option<String>,
    timestamp: Option<String>,
    amount: Option<f64>,
    amount_unit: Option<String>,
}

pub async fn connect(database_url: &str) -> Result<Pool<Sqlite>> {
    if let Some(path) = database_url.strip_prefix("sqlite://") {
        let path = Path::new(path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
    }

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await?;
    Ok(pool)
}

pub async fn migrate(pool: &Pool<Sqlite>) -> Result<()> {
    MIGRATOR.run(pool).await?;
    Ok(())
}

pub async fn load_data(pool: &Pool<Sqlite>) -> Result<SubstanceData> {
    let acquisition_rows = sqlx::query_as!(
        AcquisitionRow,
        r#"
        SELECT acquired_date, amount as "amount?", amount_unit, price as "price?", purchase_price as "purchase_price?"
        FROM possessions
        ORDER BY acquired_date, id
        "#
    )
    .fetch_all(pool)
    .await?;

    let container_rows = sqlx::query_as!(
        ContainerRow,
        r#"
        SELECT id as "id!", label, tare_weight_g as "tare_weight_g?"
        FROM possessions_containers
        ORDER BY id
        "#
    )
    .fetch_all(pool)
    .await?;

    let usage_rows = sqlx::query_as!(
        UsageRow,
        r#"
        SELECT type as "entry_type!", start_date, end_date, timestamp, amount as "amount?", amount_unit
        FROM possessions_usage
        ORDER BY COALESCE(timestamp, start_date), id
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(SubstanceData {
        acquisition: acquisition_rows
            .into_iter()
            .map(|row| Acquisition {
                acquire_date: row.acquired_date,
                value_g: row.amount.unwrap_or(0.0),
                unit: row.amount_unit,
                cost: row
                    .price
                    .or(row.purchase_price)
                    .map(|value| value.to_string()),
            })
            .collect(),
        containers: container_rows
            .into_iter()
            .map(|row| Container {
                id: row.id,
                label: row.label,
                tare_weight_g: row.tare_weight_g.unwrap_or(0.0),
            })
            .collect(),
        usage_log: usage_rows
            .into_iter()
            .map(|row| UsageEntry {
                r#type: row.entry_type,
                start_date: row.start_date,
                end_date: row.end_date,
                timestamp: row.timestamp,
                amount: row.amount.unwrap_or(0.0),
                amount_unit: row.amount_unit,
            })
            .collect(),
    })
}

pub async fn add_usage(pool: &Pool<Sqlite>, amount: f64, unit: &str) -> Result<UsageEntry> {
    let timestamp = Utc::now().date_naive().format("%Y-%m-%d").to_string();

    sqlx::query!(
        r#"
        INSERT INTO possessions_usage (type, timestamp, amount, amount_unit)
        VALUES (?, ?, ?, ?)
        "#,
        "event",
        timestamp,
        amount,
        unit
    )
    .execute(pool)
    .await?;

    Ok(UsageEntry {
        r#type: "event".to_string(),
        start_date: None,
        end_date: None,
        timestamp: Some(timestamp),
        amount,
        amount_unit: Some(unit.to_string()),
    })
}

pub async fn backup_possessions(pool: &Pool<Sqlite>, output: &Path) -> Result<()> {
    let rows = sqlx::query!(
        r#"
        SELECT acquired_date, amount as "amount?", amount_unit, price as "price?", purchase_price as "purchase_price?"
        FROM possessions
        ORDER BY acquired_date, id
        "#
    )
    .fetch_all(pool)
    .await?;

    let possessions = rows
        .into_iter()
        .map(|row| {
            let mut value = serde_json::Map::new();
            if let Some(acquired_date) = row.acquired_date {
                value.insert(
                    "acquired_date".to_string(),
                    serde_json::Value::String(acquired_date),
                );
            }
            if let Some(amount) = row.amount {
                value.insert("amount".to_string(), serde_json::json!(amount));
            }
            if let Some(amount_unit) = row.amount_unit {
                value.insert(
                    "amount_unit".to_string(),
                    serde_json::Value::String(amount_unit),
                );
            }
            if let Some(price) = row.price.or(row.purchase_price) {
                value.insert("price".to_string(), serde_json::json!(price));
            }
            serde_json::Value::Object(value)
        })
        .collect::<Vec<_>>();

    let payload = serde_json::json!({ "possessions": possessions });
    tokio::fs::write(output, serde_json::to_vec_pretty(&payload)?).await?;
    Ok(())
}
