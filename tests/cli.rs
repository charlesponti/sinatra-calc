use assert_cmd::Command;
use httpmock::prelude::*;
use predicates::str::contains;
use serde_json::json;
use std::fs;
use tempfile::TempDir;

fn mock_nominatim_server() -> (MockServer, String) {
    let server = MockServer::start();
    let base_url = server.base_url();
    (server, base_url)
}

#[test]
fn shows_help_text() {
    Command::cargo_bin("geo")
        .expect("binary exists")
        .arg("--help")
        .assert()
        .success()
        .stdout(contains("A CLI for looking up place coordinates"));
}

#[test]
fn shows_subcommand_help() {
    Command::cargo_bin("geo")
        .expect("binary exists")
        .args(["geocode", "--help"])
        .assert()
        .success()
        .stdout(contains("Lookup a place name using OSM Nominatim"));
}

#[test]
fn geocode_success() {
    let (server, base_url) = mock_nominatim_server();

    server.mock(|when, then| {
        when.method(GET)
            .path("/search")
            .query_param("q", "Paris, France");
        then.status(200).json_body(json!([
            {
                "display_name": "Paris, France",
                "lat": "48.8566",
                "lon": "2.3522",
                "address": {
                    "city": "Paris",
                    "state": "Île-de-France",
                    "country": "France",
                    "country_code": "fr"
                }
            }
        ]));
    });

    Command::cargo_bin("geo")
        .expect("binary exists")
        .args(["geocode", "Paris, France"])
        .env("NOMINATIM_BASE_URL", &base_url)
        .assert()
        .success()
        .stdout(contains("Paris, France"))
        .stdout(contains("lat=48.8566"))
        .stdout(contains("lon=2.3522"));
}

#[test]
fn geocode_not_found() {
    let (server, base_url) = mock_nominatim_server();

    server.mock(|when, then| {
        when.method(GET)
            .path("/search")
            .query_param("q", "NonexistentPlace12345");
        then.status(200).json_body(json!([]));
    });

    Command::cargo_bin("geo")
        .expect("binary exists")
        .args(["geocode", "NonexistentPlace12345"])
        .env("NOMINATIM_BASE_URL", &base_url)
        .assert()
        .success()
        .stdout(contains("no results for 'NonexistentPlace12345'"));
}

#[test]
fn geocode_csv_creates_output() {
    let (server, base_url) = mock_nominatim_server();

    server.mock(|when, then| {
        when.method(GET).path("/search");
        then.status(200).json_body(json!([
            {
                "display_name": "Paris, France",
                "lat": "48.8566",
                "lon": "2.3522",
                "address": {
                    "city": "Paris",
                    "state": "Île-de-France",
                    "country": "France",
                    "country_code": "fr"
                }
            }
        ]));
    });

    let temp_dir = TempDir::new().expect("create temp dir");
    let input_path = temp_dir.path().join("input.csv");
    let output_path = temp_dir.path().join("output.csv");

    fs::write(&input_path, "Name,Notes\n\"Paris, France\",Test place\n").expect("write input CSV");

    Command::cargo_bin("geo")
        .expect("binary exists")
        .args([
            "geocode-csv",
            "-f",
            input_path.to_str().unwrap(),
            "-c",
            "Name",
            "-o",
            output_path.to_str().unwrap(),
        ])
        .env("NOMINATIM_BASE_URL", &base_url)
        .assert()
        .success();

    assert!(output_path.exists(), "output file should be created");
}

#[test]
fn geocode_csv_has_required_columns() {
    let (server, base_url) = mock_nominatim_server();

    server.mock(|when, then| {
        when.method(GET).path("/search");
        then.status(200).json_body(json!([
            {
                "display_name": "Paris, France",
                "lat": "48.8566",
                "lon": "2.3522",
                "address": {
                    "city": "Paris",
                    "state": "Île-de-France",
                    "country": "France",
                    "country_code": "fr"
                }
            }
        ]));
    });

    let temp_dir = TempDir::new().expect("create temp dir");
    let input_path = temp_dir.path().join("input.csv");
    let output_path = temp_dir.path().join("output.csv");

    fs::write(&input_path, "Name,Notes\n\"Paris, France\",Test\n").expect("write input CSV");

    Command::cargo_bin("geo")
        .expect("binary exists")
        .args([
            "geocode-csv",
            "-f",
            input_path.to_str().unwrap(),
            "-c",
            "Name",
            "-o",
            output_path.to_str().unwrap(),
        ])
        .env("NOMINATIM_BASE_URL", &base_url)
        .assert()
        .success();

    let output = fs::read_to_string(&output_path).expect("read output CSV");
    let headers: Vec<&str> = output.lines().next().unwrap().split(',').collect();

    assert!(headers.contains(&"lat"), "output should have lat column");
    assert!(headers.contains(&"lon"), "output should have lon column");
    assert!(headers.contains(&"city"), "output should have city column");
    assert!(
        headers.contains(&"state"),
        "output should have state column"
    );
    assert!(
        headers.contains(&"country"),
        "output should have country column"
    );
    assert!(
        headers.contains(&"country_code"),
        "output should have country_code column"
    );
}

#[test]
fn geocode_csv_preserves_data() {
    let (server, base_url) = mock_nominatim_server();

    server.mock(|when, then| {
        when.method(GET).path("/search");
        then.status(200).json_body(json!([
            {
                "display_name": "Paris, France",
                "lat": "48.8566",
                "lon": "2.3522",
                "address": {
                    "city": "Paris",
                    "state": "Île-de-France",
                    "country": "France",
                    "country_code": "fr"
                }
            }
        ]));
    });

    let temp_dir = TempDir::new().expect("create temp dir");
    let input_path = temp_dir.path().join("input.csv");
    let output_path = temp_dir.path().join("output.csv");

    fs::write(
        &input_path,
        "Name,Notes,Rating\n\"Paris, France\",Test place,5\n",
    )
    .expect("write input CSV");

    Command::cargo_bin("geo")
        .expect("binary exists")
        .args([
            "geocode-csv",
            "-f",
            input_path.to_str().unwrap(),
            "-c",
            "Name",
            "-o",
            output_path.to_str().unwrap(),
        ])
        .env("NOMINATIM_BASE_URL", &base_url)
        .assert()
        .success();

    let output = fs::read_to_string(&output_path).expect("read output CSV");
    let headers: Vec<&str> = output.lines().next().unwrap().split(',').collect();

    assert!(headers.contains(&"Name"), "Name column should be preserved");
    assert!(
        headers.contains(&"Notes"),
        "Notes column should be preserved"
    );
    assert!(
        headers.contains(&"Rating"),
        "Rating column should be preserved"
    );
}
