use assert_cmd::Command;
use std::fs;

fn artifacts_dir() -> std::path::PathBuf {
    let dir = std::path::PathBuf::from("tests/artifacts");
    fs::create_dir_all(&dir).ok();
    dir
}

#[ignore]
#[test]
fn e2e_geocode_paris() {
    let artifacts = artifacts_dir();
    let stdout_file = artifacts.join("e2e_geocode_paris.stdout.txt");

    let output = Command::cargo_bin("geo")
        .expect("binary exists")
        .args(["geocode", "Paris, France"])
        .output()
        .expect("run command");

    fs::write(&stdout_file, output.stdout.clone()).ok();

    assert!(
        output.status.success(),
        "command failed with: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("Paris"),
        "stdout should contain Paris"
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("lat="),
        "stdout should contain lat="
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("lon="),
        "stdout should contain lon="
    );

    println!("Output saved to: {}", stdout_file.display());
}

#[ignore]
#[test]
fn e2e_geocode_csv() {
    let artifacts = artifacts_dir();
    let input_path = artifacts.join("e2e_input.csv");
    let output_path = artifacts.join("e2e_output.csv");

    fs::write(
        &input_path,
        "Name,Notes\n\"Paris, France\",Test place\n\"Brooklyn, New York\",Another place\n",
    )
    .expect("write input CSV");

    let result = Command::cargo_bin("geo")
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
        .output()
        .expect("run command");

    assert!(
        result.status.success(),
        "command failed with: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(output_path.exists(), "output file should be created");

    let output = fs::read_to_string(&output_path).expect("read output CSV");
    let headers: Vec<&str> = output.lines().next().unwrap().split(',').collect();

    assert!(headers.contains(&"lat"), "output should have lat column");
    assert!(headers.contains(&"lon"), "output should have lon column");
    assert!(headers.contains(&"city"), "output should have city column");
    assert!(
        headers.contains(&"country"),
        "output should have country column"
    );

    println!("Input saved to:  {}", input_path.display());
    println!("Output saved to:  {}", output_path.display());
    println!("--- Output CSV ---");
    println!("{}", output);
}
