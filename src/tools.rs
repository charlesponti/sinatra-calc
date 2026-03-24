use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use csv::{Reader, ReaderBuilder, StringRecord, Writer};
use serde_json::Value;
use walkdir::WalkDir;

/// Convert YAML/JSON/CSV files in `input_dir` into CSV files.
///
/// This is the shared logic used by the `convert_csv` binary and the
/// `mater convert-csv` subcommand.
pub fn convert_csv(input_dir: &Path) -> Result<()> {
    for entry in WalkDir::new(input_dir)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        match path.extension().and_then(|e| e.to_str()) {
            Some("yml") | Some("yaml") => convert_yaml(path)?,
            Some("json") => convert_json(path)?,
            Some("csv") => extract_logs_from_csv(path)?,
            _ => {}
        }
    }

    Ok(())
}

fn convert_yaml(path: &Path) -> Result<()> {
    let text = fs::read_to_string(path)?;
    let doc: serde_yaml::Value = serde_yaml::from_str(&text)?;
    let json = serde_json::to_value(doc)?;
    convert_json_value(path, &json)
}

fn convert_json(path: &Path) -> Result<()> {
    let text = fs::read_to_string(path)?;
    let json: Value = serde_json::from_str(&text)?;
    convert_json_value(path, &json)
}

fn convert_json_value(path: &Path, json: &Value) -> Result<()> {
    if let Some(array) = json.as_array() {
        write_csv(path.with_extension("csv"), array, None)?;
        return Ok(());
    }

    let obj = match json.as_object() {
        Some(o) => o,
        None => {
            // Not object/array, just write a single-value CSV
            let out = path.with_extension("csv");
            let mut w = Writer::from_path(out)?;
            w.write_record(&["value"])?;
            w.write_record(&[json.to_string()])?;
            w.flush()?;
            return Ok(());
        }
    };

    // Find all array-valued keys.
    let arrays: Vec<(&String, &Value)> = obj.iter().filter(|(_, v)| v.is_array()).collect();

    if arrays.len() == 1 {
        let (key, value) = arrays[0];
        let out = path.with_file_name(format!(
            "{}-{}.csv",
            path.file_stem().unwrap().to_string_lossy(),
            key
        ));
        write_csv(out, value.as_array().unwrap(), Some(key))?;
        return Ok(());
    }

    if arrays.len() > 1 {
        for (key, value) in arrays {
            let out = path.with_file_name(format!(
                "{}-{}.csv",
                path.file_stem().unwrap().to_string_lossy(),
                key
            ));
            write_csv(out, value.as_array().unwrap(), Some(key))?;
        }
        return Ok(());
    }

    // No arrays: treat as single row object.
    let out = path.with_extension("csv");
    write_csv(out, &[json.clone()], None)
}

fn write_csv(path: PathBuf, rows: &[Value], prefix: Option<&str>) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut headers = BTreeSet::new();
    for row in rows.iter() {
        if let Some(obj) = row.as_object() {
            for k in obj.keys() {
                headers.insert(k.clone());
            }
        }
    }

    let headers: Vec<String> = headers.into_iter().collect();

    let mut writer = Writer::from_path(&path)?;

    let mut header_row: Vec<String> = Vec::new();
    if prefix.is_some() {
        header_row.push("_source".to_string());
    }
    header_row.extend(headers.clone());
    writer.write_record(&header_row)?;

    // If the source rows include a JSON-encoded array field named `logs`,
    // extract it into a separate CSV where each element becomes a row.
    let mut extracted_logs: Vec<(Vec<String>, HashMap<String, String>)> = Vec::new();
    let mut log_headers: BTreeSet<String> = BTreeSet::new();

    for row in rows.iter() {
        let mut record: Vec<String> = Vec::new();
        let mut parent_values: Vec<String> = Vec::new();
        let mut parent_values_for_logs: Vec<String> = Vec::new();
        if let Some(source) = prefix {
            record.push(source.to_string());
            parent_values_for_logs.push(source.to_string());
        }

        let obj = row.as_object();

        for key in &headers {
            let raw_value = obj.and_then(|o| o.get(key));
            let value = raw_value
                .map(|v| match v {
                    Value::Null => "".to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();

            if key == "logs" {
                if let Some(Value::String(s)) = raw_value {
                    if let Ok(log_items) = serde_json::from_str::<Vec<Value>>(s) {
                        for item in log_items {
                            if let Some(obj) = item.as_object() {
                                let mut log_values: HashMap<String, String> = HashMap::new();
                                for (k, v) in obj {
                                    log_headers.insert(k.clone());
                                    let value_str = match v {
                                        Value::String(s) => s.clone(),
                                        Value::Number(n) => n.to_string(),
                                        Value::Bool(b) => b.to_string(),
                                        Value::Null => "".to_string(),
                                        other => other.to_string(),
                                    };
                                    log_values.insert(k.clone(), value_str);
                                }
                                extracted_logs.push((parent_values_for_logs.clone(), log_values));
                            }
                        }
                    }
                }
                // Keep column count consistent.
                parent_values.push("".to_string());
                record.push("".to_string());
            } else {
                parent_values.push(value.clone());
                parent_values_for_logs.push(value.clone());
                record.push(value);
            }
        }

        writer.write_record(&record)?;
    }

    writer.flush()?;

    if !extracted_logs.is_empty() {
        let base = path.with_file_name(format!(
            "{}-logs.csv",
            path.file_stem().unwrap().to_string_lossy()
        ));
        let mut log_writer = Writer::from_path(base)?;

        let mut log_header_row: Vec<String> = Vec::new();
        if prefix.is_some() {
            log_header_row.push("_source".to_string());
        }
        log_header_row.extend(headers.iter().filter(|k| *k != "logs").cloned());
        log_header_row.extend(log_headers.clone());
        log_writer.write_record(&log_header_row)?;

        for (parent_values, log_values) in extracted_logs {
            let mut row = parent_values;
            for header in log_headers.iter() {
                row.push(log_values.get(header).cloned().unwrap_or_default());
            }
            log_writer.write_record(&row)?;
        }

        log_writer.flush()?;
    }

    Ok(())
}

fn extract_logs_from_csv(path: &Path) -> Result<()> {
    let mut rdr = Reader::from_path(path)?;
    let headers = rdr.headers()?.clone();
    let log_col = headers.iter().position(|h| h == "logs");
    let log_col = match log_col {
        Some(i) => i,
        None => return Ok(()),
    };

    let mut extracted: Vec<(Vec<String>, HashMap<String, String>)> = Vec::new();
    let mut log_headers: BTreeSet<String> = BTreeSet::new();

    for result in rdr.records() {
        let record = result?;
        let parent_values: Vec<String> = record
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if i == log_col { None } else { Some(v.to_string()) })
            .collect();

        if let Some(raw) = record.get(log_col) {
            if let Ok(log_items) = serde_json::from_str::<Vec<Value>>(raw) {
                for item in log_items {
                    if let Some(obj) = item.as_object() {
                        let mut log_map: HashMap<String, String> = HashMap::new();
                        for (k, v) in obj {
                            log_headers.insert(k.clone());
                            let value_str = match v {
                                Value::String(s) => s.clone(),
                                Value::Number(n) => n.to_string(),
                                Value::Bool(b) => b.to_string(),
                                Value::Null => "".to_string(),
                                other => other.to_string(),
                            };
                            log_map.insert(k.clone(), value_str);
                        }
                        extracted.push((parent_values.clone(), log_map));
                    }
                }
            }
        }
    }

    if extracted.is_empty() {
        return Ok(());
    }

    let out_path = path.with_file_name(format!(
        "{}-logs.csv",
        path.file_stem().unwrap().to_string_lossy()
    ));
    let mut w = Writer::from_path(out_path)?;

    let mut header_row: Vec<String> = Vec::new();
    header_row.extend(
        headers
            .iter()
            .enumerate()
            .filter_map(|(i, h)| if i == log_col { None } else { Some(h.to_string()) }),
    );
    header_row.extend(log_headers.clone());
    w.write_record(&header_row)?;

    for (parent_values, log_map) in extracted {
        let mut row = parent_values;
        for header in log_headers.iter() {
            row.push(log_map.get(header).cloned().unwrap_or_default());
        }
        w.write_record(&row)?;
    }

    w.flush()?;

    Ok(())
}

/// Unify CSV files from a directory into a single analytics-friendly CSV.
///
/// This is shared by the `unify_csv` binary and the `mater unify-csv` subcommand.
pub fn unify_csv(input_dir: &Path, output: &Path) -> Result<()> {
    let mut writer = Writer::from_path(output)?;

    let headers = vec![
        "record_source",
        "record_type",
        "id",
        "name",
        "date",
        "start_date",
        "end_date",
        "timestamp",
        "notes",
        "amount",
        "unit",
        "rating",
        "percent",
        "properties_json",
    ];

    writer.write_record(headers.clone())?;

    for entry in WalkDir::new(input_dir).max_depth(1).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        if path.extension().and_then(|e| e.to_str()) != Some("csv") {
            continue;
        }

        if path == output {
            continue;
        }

        process_csv(path, &mut writer)?;
    }

    writer.flush()?;
    Ok(())
}

fn process_csv(path: &Path, writer: &mut Writer<std::fs::File>) -> Result<()> {
    let file_stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default();

    let mut reader = ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = reader.headers()?.clone();

    for result in reader.records() {
        let record = result?;
        let unified = unify_record(file_stem, &headers, &record);
        writer.write_record(unified)?;
    }

    Ok(())
}

fn unify_record(file_stem: &str, headers: &StringRecord, record: &StringRecord) -> Vec<String> {
    let row: Vec<(String, String)> = headers
        .iter()
        .zip(record.iter())
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let mut values: serde_json::Map<String, Value> = serde_json::Map::new();
    for (k, v) in row {
        if !v.is_empty() {
            values.insert(k, Value::String(v));
        }
    }

    // Determine canonical fields.
    let record_source = file_stem.to_string();
    let record_type = values
        .get("_source")
        .and_then(|v| v.as_str())
        .unwrap_or(&record_source)
        .to_string();

    let id = values
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let name = values
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .or_else(|| {
            values
                .get("title")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        })
        .unwrap_or_default();

    let date = values
        .get("date")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let start_date = values
        .get("start_date")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let end_date = values
        .get("end_date")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let timestamp = values
        .get("timestamp")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let notes = values
        .get("notes")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let amount = values
        .get("amount")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let unit = values
        .get("unit")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let rating = values
        .get("rating")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let percent = values
        .get("percent")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let properties_json = values
        .get("properties_json")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    vec![
        record_source,
        record_type,
        id,
        name,
        date,
        start_date,
        end_date,
        timestamp,
        notes,
        amount,
        unit,
        rating,
        percent,
        properties_json,
    ]
}
