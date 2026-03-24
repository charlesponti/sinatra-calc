use assert_cmd::Command;
use predicates::prelude::*;
use sqlx::{Connection, SqliteConnection};
use std::fs::File;
use csv;

fn sqlite_url(path: &std::path::Path) -> String {
    format!("sqlite://{}", path.display())
}

async fn setup_db(path: &std::path::Path) {
    File::create(path).unwrap();
    let url = sqlite_url(path);
    let mut conn = SqliteConnection::connect(&url).await.unwrap();
    sqlx::query(include_str!("../migrations/0001_init.sql"))
        .execute(&mut conn)
        .await
        .unwrap();
}

#[test]
fn help_shows_expected_commands() {
    Command::cargo_bin("mater")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("add"))
        .stdout(predicate::str::contains("backup"))
        .stdout(predicate::str::contains("SQLite"));
}

#[test]
fn status_runs_with_sqlite_database() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let url = sqlite_url(&db_path);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        setup_db(&db_path).await;
        let mut conn = SqliteConnection::connect(&url).await.unwrap();
        sqlx::query(
            "INSERT INTO possessions (acquired_date, amount, amount_unit) VALUES (?, ?, ?)"
        )
        .bind("2026-01-01")
        .bind(10.0_f64)
        .bind("g")
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO possessions_usage (type, timestamp, amount, amount_unit) VALUES (?, ?, ?, ?)"
        )
        .bind("event")
        .bind("2026-01-01")
        .bind(1.0_f64)
        .bind("g")
        .execute(&mut conn)
        .await
        .unwrap();
    });

    Command::cargo_bin("mater")
        .unwrap()
        .args(["--database", db_path.to_str().unwrap(), "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("total acquired"))
        .stdout(predicate::str::contains("remaining"));
}

#[test]
fn add_persists_usage_to_sqlite() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        setup_db(&db_path).await;
    });

    Command::cargo_bin("mater")
        .unwrap()
        .args(["--database", db_path.to_str().unwrap(), "add", "0.25"])
        .assert()
        .success()
        .stdout(predicate::str::contains("added usage entry"));

    runtime.block_on(async {
        let url = sqlite_url(&db_path);
        let mut conn = SqliteConnection::connect(&url).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM possessions_usage")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(count, 1);
    });
}

#[test]
fn format_date_normalizes_csv_dates() {
    let temp_dir = tempfile::tempdir().unwrap();
    let csv_path = temp_dir.path().join("dates.csv");
    std::fs::write(
        &csv_path,
        "id,date\n1,\"April 8, 2017\"\n2,\"2026-03-16\"\n",
    )
    .unwrap();

    Command::cargo_bin("mater")
        .unwrap()
        .args(["format-date", "-f", csv_path.to_str().unwrap(), "-c", "date"])
        .assert()
        .success();

    let mut rdr = csv::Reader::from_path(&csv_path).unwrap();
    let records: Vec<_> = rdr
        .records()
        .map(|r| r.unwrap().get(1).unwrap().to_string())
        .collect();

    assert_eq!(records, vec!["2017-04-08", "2026-03-16"]);
}
