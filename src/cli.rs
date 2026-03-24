use std::{fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use serde::Deserialize;

use crate::{
    db::{DEFAULT_DATABASE_PATH, add_usage, backup_possessions, connect, load_data, migrate},
    state::{compute_state, parse_flexible_date, parse_yyyy_mm_dd},
    tools::{convert_csv, unify_csv},
};

#[derive(Parser)]
#[command(
    author,
    version,
    about = "mater CLI (tracking port)",
    long_about = "A small CLI for tracking acquisition/use of substances backed by SQLite with typed SQL queries.",
    arg_required_else_help = true
)]
pub struct Cli {
    /// Path to the SQLite database file.
    #[arg(long, default_value = DEFAULT_DATABASE_PATH, global = true)]
    pub database: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Show current totals (and optionally remaining at a past date)
    Status {
        /// Show the remaining amount as of the given date (YYYY-MM-DD)
        #[arg(long)]
        date: Option<String>,
    },
    /// Record a new usage event (appends to the log)
    Add {
        /// Amount to record
        amount: f64,
        /// Unit of the amount (default: g)
        #[arg(long, default_value = "g")]
        unit: String,
    },
    /// Export the current "possessions" list to a JSON file (db format)
    Backup {
        /// Output file path
        #[arg(long, default_value = "possessions-backup.json")]
        output: PathBuf,
    },
    /// Normalize date strings in a CSV file to YYYY-MM-DD.
    FormatDate {
        /// Input CSV file to transform.
        #[arg(short = 'f', long)]
        file: PathBuf,
        /// Column(s) to normalize. Can be provided multiple times or as a comma-delimited list.
        #[arg(short = 'c', long, value_delimiter = ',')]
        columns: Vec<String>,
    },
    /// Convert tracking YAML/JSON/CSV files into CSV format.
    ConvertCsv {
        /// Directory containing source files.
        #[arg(long, default_value = "../tracking")]
        input: PathBuf,
    },
    /// Unify multiple tracking CSV files into a single CSV.
    UnifyCsv {
        /// Directory containing source CSV files.
        #[arg(long, default_value = "../tracking")]
        input: PathBuf,
        /// Output unified CSV.
        #[arg(long, default_value = "../tracking/unified.csv")]
        output: PathBuf,
    },
    /// Lookup a place name using OSM Nominatim and print coordinates.
    Geocode {
        /// The query to look up (e.g. "Mahopac, New York").
        query: String,
    },
    /// Geocode a CSV column (adds lat/lon columns).
    GeocodeCsv {
        /// Input CSV file.
        #[arg(short = 'f', long)]
        file: PathBuf,
        /// Column to geocode (e.g. "Name" or "City").
        #[arg(short = 'c', long)]
        column: String,
        /// Output CSV file (defaults to <input>.geocoded.csv).
        #[arg(short = 'o', long)]
        output: Option<PathBuf>,
    },
    /// Fix common typos in places.cities.csv.
    FixPlaceCitiesTypos,
    /// Geocode places.cities.csv and update the file in-place.
    GeocodePlaceCities,
    /// Normalize columns in places.cities.csv.
    NormalizePlaceCities,
}

#[derive(Deserialize)]
struct NominatimResult {
    display_name: String,
    lat: String,
    lon: String,
    address: Option<NominatimAddress>,
}

#[derive(Deserialize)]
struct NominatimAddress {
    city: Option<String>,
    town: Option<String>,
    village: Option<String>,
    hamlet: Option<String>,
    state: Option<String>,
    country: Option<String>,
    country_code: Option<String>,
}

impl NominatimAddress {
    fn best_city(&self) -> Option<&String> {
        self.city
            .as_ref()
            .or_else(|| self.town.as_ref())
            .or_else(|| self.village.as_ref())
            .or_else(|| self.hamlet.as_ref())
    }
}

async fn geocode_nominatim(query: &str) -> Result<Option<NominatimResult>> {
    let url = format!(
        "https://nominatim.openstreetmap.org/search?format=json&limit=1&addressdetails=1&q={}",
        urlencoding::encode(query)
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    let res = match client
        .get(&url)
        .header("User-Agent", "mater-cli/1.0 (contact: none)")
        .send()
        .await
    {
        Ok(r) => r,
        Err(err) => {
            eprintln!("geocode timeout/failure for '{}': {}", query, err);
            return Ok(None);
        }
    };

    let res = match res.error_for_status() {
        Ok(r) => r,
        Err(err) => {
            eprintln!("geocode HTTP error for '{}': {}", query, err);
            return Ok(None);
        }
    };

    let results: Vec<NominatimResult> = match res.json().await {
        Ok(v) => v,
        Err(err) => {
            eprintln!("geocode parse error for '{}': {}", query, err);
            return Ok(None);
        }
    };

    Ok(results.into_iter().next())
}

async fn geocode_csv(file: &PathBuf, column: &str, output: &PathBuf) -> Result<()> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(file)
        .with_context(|| format!("failed to open {}", file.display()))?;

    let headers = reader
        .headers()
        .with_context(|| format!("failed to read headers from {}", file.display()))?
        .clone();

    let col_index = headers
        .iter()
        .position(|h| h == column)
        .ok_or_else(|| anyhow!("column not found: {}", column))?;

    // Determine existing column indices, and add any missing columns to the header.
    let mut out_headers = headers.clone();
    let index = |name: &str| out_headers.iter().position(|h| h == name);

    let mut lat_idx = index("lat");
    let mut lon_idx = index("lon");
    let mut city_idx = index("city");
    let mut state_idx = index("state");
    let mut country_idx = index("country");
    let mut country_code_idx = index("country_code");

    for (name, slot) in [
        ("lat", &mut lat_idx),
        ("lon", &mut lon_idx),
        ("city", &mut city_idx),
        ("state", &mut state_idx),
        ("country", &mut country_idx),
        ("country_code", &mut country_code_idx),
    ] {
        if slot.is_none() {
            out_headers.push_field(name);
            *slot = Some(out_headers.len() - 1);
        }
    }

    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(output)
        .with_context(|| format!("failed to create output file {}", output.display()))?;

    writer.write_record(&out_headers)?;

    let mut cache: std::collections::HashMap<
        String,
        (String, String, String, String, String, String),
    > = Default::default();

    for result in reader.records() {
        let record = result.with_context(|| format!("failed to read record from {}", file.display()))?;
        let mut out_record: Vec<String> = record.iter().map(|v| v.to_string()).collect();

        // Ensure the record has enough columns for all output headers.
        if out_record.len() < out_headers.len() {
            out_record.resize(out_headers.len(), String::new());
        }

        let query = out_record
            .get(col_index)
            .map(|s| s.trim())
            .unwrap_or("");

        let (lat, lon, city, state, country, country_code) = if query.is_empty() {
            (
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "".to_string(),
            )
        } else if let Some((lat, lon, city, state, country, country_code)) = cache.get(query) {
            (
                lat.clone(),
                lon.clone(),
                city.clone(),
                state.clone(),
                country.clone(),
                country_code.clone(),
            )
        } else {
            // Nominatim policy wants <=1 request/sec.
            tokio::time::sleep(Duration::from_millis(1100)).await;
            if let Some(place) = geocode_nominatim(query).await? {
                let lat = place.lat.clone();
                let lon = place.lon.clone();
                let city = place
                    .address
                    .as_ref()
                    .and_then(|a| a.best_city())
                    .cloned()
                    .unwrap_or_default();
                let state = place
                    .address
                    .as_ref()
                    .and_then(|a| a.state.clone())
                    .unwrap_or_default();
                let country = place
                    .address
                    .as_ref()
                    .and_then(|a| a.country.clone())
                    .unwrap_or_default();
                let country_code = place
                    .address
                    .as_ref()
                    .and_then(|a| a.country_code.clone())
                    .unwrap_or_default();
                cache.insert(
                    query.to_string(),
                    (
                        lat.clone(),
                        lon.clone(),
                        city.clone(),
                        state.clone(),
                        country.clone(),
                        country_code.clone(),
                    ),
                );
                (lat, lon, city, state, country, country_code)
            } else {
                (
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                )
            }
        };

        if let Some(idx) = lat_idx {
            out_record[idx] = lat;
        }
        if let Some(idx) = lon_idx {
            out_record[idx] = lon;
        }
        if let Some(idx) = city_idx {
            out_record[idx] = city;
        }
        if let Some(idx) = state_idx {
            out_record[idx] = state;
        }
        if let Some(idx) = country_idx {
            out_record[idx] = country;
        }
        if let Some(idx) = country_code_idx {
            out_record[idx] = country_code;
        }

        writer.write_record(&out_record)?;
    }

    writer.flush()?;
    Ok(())
}

fn fix_places_cities_typos() -> Result<()> {
    let tracking_dir = std::path::PathBuf::from(
        std::env::var("TRACKING_DIR")
            .unwrap_or_else(|_| format!("{}/Documents/tracking", std::env::var("HOME").unwrap_or_default()))
    );
    let input_path = tracking_dir.join("places.cities.csv");
    let tmp_path = tracking_dir.join("places.cities.typos-fixed.tmp.csv");

    // Common misspellings observed in the dataset.
    let fixes = [
        ("Perpignanc, France", "Perpignan, France"),
        ("Southhampton, New York", "Southampton, New York"),
        ("Austurias", "Asturias"),
        ("Bilboa", "Bilbao"),
        ("San Sebastian", "San Sebastián"),
        ("Cordoba", "Córdoba"),
    ];

    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(&input_path)
        .with_context(|| format!("failed to open {}", input_path.display()))?;

    let headers = reader
        .headers()
        .with_context(|| format!("failed to read headers from {}", input_path.display()))?
        .clone();

    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(&tmp_path)
        .with_context(|| format!("failed to create temp file {}", tmp_path.display()))?;

    writer.write_record(&headers)?;

    for result in reader.records() {
        let record = result.with_context(|| format!("failed to read record from {}", input_path.display()))?;
        let mut out_record: Vec<String> = record.iter().map(|v| v.to_string()).collect();

        // Find the Name column index.
        if let Some(name_idx) = headers.iter().position(|h| h == "Name") {
            if let Some(name_val) = out_record.get_mut(name_idx) {
                let unquoted = name_val.trim_matches('"');
                for (bad, good) in fixes.iter() {
                    if unquoted == *bad {
                        *name_val = good.to_string();
                        break;
                    }
                }
            }
        }
        writer.write_record(&out_record)?;
    }
    writer.flush()?;

    // Replace the original file
    fs::rename(&tmp_path, &input_path)
        .with_context(|| format!("failed to replace {} with {}", input_path.display(), tmp_path.display()))?;
    println!("fixed typos in {}", input_path.display());

    Ok(())
}

async fn geocode_places_cities() -> Result<()> {
    let tracking_dir = std::path::PathBuf::from(
        std::env::var("TRACKING_DIR")
            .unwrap_or_else(|_| format!("{}/Documents/tracking", std::env::var("HOME").unwrap_or_default()))
    );
    let csv = tracking_dir.join("places.cities.csv");
    let tmp = tracking_dir.join("places.cities.geocoded.tmp.csv");

    geocode_csv(&csv, "Name", &tmp).await?;
    fs::rename(&tmp, &csv)
        .with_context(|| format!("failed to replace {} with {}", csv.display(), tmp.display()))?;
    println!("updated {}", csv.display());

    Ok(())
}

fn normalize_places_cities() -> Result<()> {
    let tracking_dir = std::path::PathBuf::from(
        std::env::var("TRACKING_DIR")
            .unwrap_or_else(|_| format!("{}/Documents/tracking", std::env::var("HOME").unwrap_or_default()))
    );
    let input_path = tracking_dir.join("places.cities.csv");
    let tmp_path = tracking_dir.join("places.cities.normalized.tmp.csv");

    // Define the desired final column order.
    let final_columns = vec![
        "Name", "Continent", "Status", "lat", "lon", "city", "state", "country", "country_code",
    ];

    // Basic mapping from ISO country code -> continent.
    let continent_by_country: std::collections::HashMap<&str, &str> = [
        ("US", "North America"), ("CA", "North America"), ("MX", "North America"),
        ("BR", "South America"), ("AR", "South America"),
        ("GB", "Europe"), ("FR", "Europe"), ("DE", "Europe"), ("IT", "Europe"),
        ("ES", "Europe"), ("NL", "Europe"), ("BE", "Europe"), ("NO", "Europe"),
        ("SE", "Europe"), ("FI", "Europe"), ("DK", "Europe"), ("CH", "Europe"),
        ("AT", "Europe"), ("IE", "Europe"), ("PT", "Europe"), ("GR", "Europe"),
        ("JP", "Asia"), ("CN", "Asia"), ("SG", "Asia"), ("ID", "Asia"),
        ("AU", "Oceania"), ("NZ", "Oceania"), ("IN", "Asia"), ("KR", "Asia"), ("RU", "Europe"),
    ].iter().copied().collect();

    fn continent_from_latlon(lat: &str, lon: &str) -> String {
        match (lat.parse::<f64>(), lon.parse::<f64>()) {
            (Ok(lat), Ok(lon)) => {
                if lat < -60.0 {
                    "Antarctica".to_string()
                } else if lon >= -170.0 && lon <= -50.0 && lat >= 5.0 && lat <= 85.0 {
                    "North America".to_string()
                } else if lon >= -90.0 && lon <= -30.0 && lat >= -60.0 && lat <= 15.0 {
                    "South America".to_string()
                } else if lon >= -25.0 && lon <= 45.0 && lat >= 34.0 && lat <= 72.0 {
                    "Europe".to_string()
                } else if lon >= -20.0 && lon <= 55.0 && lat >= -35.0 && lat <= 38.0 {
                    "Africa".to_string()
                } else if lon >= 26.0 && lon <= 180.0 && lat >= 0.0 && lat <= 80.0 {
                    "Asia".to_string()
                } else if lon >= 110.0 && lon <= 180.0 && lat >= -50.0 && lat <= 10.0 {
                    "Oceania".to_string()
                } else {
                    String::new()
                }
            },
            _ => String::new(),
        }
    }

    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(&input_path)
        .with_context(|| format!("failed to open {}", input_path.display()))?;

    let headers = reader
        .headers()
        .with_context(|| format!("failed to read headers from {}", input_path.display()))?
        .clone();

    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(&tmp_path)
        .with_context(|| format!("failed to create temp file {}", tmp_path.display()))?;

    writer.write_record(&final_columns)?;

    for result in reader.records() {
        let record = result.with_context(|| format!("failed to read record from {}", input_path.display()))?;

        let mut out_row = std::collections::HashMap::new();

        // Initialize with empty strings
        for col in &final_columns {
            out_row.insert(*col, String::new());
        }

        // Copy values from input record
        for (idx, val) in record.iter().enumerate() {
            if let Some(header) = headers.get(idx) {
                if header == "Country" {
                    // Skip old Country column, keep geocoded country
                    continue;
                }
                if final_columns.contains(&header.as_ref()) {
                    out_row.insert(header, val.to_string());
                }
            }
        }

        // Derive continent from country_code if missing
        if out_row.get("Continent").map(|s| s.is_empty()).unwrap_or(true) {
            if let Some(cc) = out_row.get("country_code") {
                if !cc.is_empty() {
                    let cc_upper = cc.to_uppercase();
                    if let Some(continent) = continent_by_country.get(cc_upper.as_str()) {
                        out_row.insert("Continent", continent.to_string());
                    }
                }
            }
        }

        // Fallback to lat/lon bounding boxes
        if out_row.get("Continent").map(|s| s.is_empty()).unwrap_or(true) {
            let lat = out_row.get("lat").map(|s| s.as_str()).unwrap_or("");
            let lon = out_row.get("lon").map(|s| s.as_str()).unwrap_or("");
            if !lat.is_empty() && !lon.is_empty() {
                let continent = continent_from_latlon(lat, lon);
                if !continent.is_empty() {
                    out_row.insert("Continent", continent);
                }
            }
        }

        // Write the normalized row in the correct column order
        let output_row: Vec<String> = final_columns
            .iter()
            .map(|col| out_row.get(*col).cloned().unwrap_or_default())
            .collect();
        writer.write_record(&output_row)?;
    }

    writer.flush()?;

    // Replace the original file
    fs::rename(&tmp_path, &input_path)
        .with_context(|| format!("failed to replace {} with {}", input_path.display(), tmp_path.display()))?;
    println!("normalized columns in {}", input_path.display());

    Ok(())
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Command::FormatDate { file, columns } => {
            eprintln!(
                "warning: `mater format-date` is deprecated; use `voidline data format-date -f {} -c {}` instead",
                file.display(),
                columns.join(",")
            );
            format_date(file, columns)?;
            return Ok(());
        }
        Command::ConvertCsv { input } => {
            eprintln!(
                "warning: `mater convert-csv` is deprecated; use `voidline data convert --input {}` instead",
                input.display()
            );
            convert_csv(input)?;
            println!("converted data in {}", input.display());
            return Ok(());
        }
        Command::UnifyCsv { input, output } => {
            eprintln!(
                "warning: `mater unify-csv` is deprecated; use `voidline data unify --input {} --output {}` instead",
                input.display(),
                output.display()
            );
            unify_csv(input, output)?;
            println!("wrote unified CSV to {}", output.display());
            return Ok(());
        }
        Command::Geocode { query } => {
            eprintln!(
                "warning: `mater geocode` is deprecated; use `voidline places geocode \"{}\"` instead",
                query
            );
            match geocode_nominatim(query).await? {
                Some(place) => {
                    println!("{}", place.display_name);
                    println!("lat={}, lon={}", place.lat, place.lon);
                }
                None => println!("no results for '{}'", query),
            }
            return Ok(());
        }
        Command::GeocodeCsv { file, column, output } => {
            let output = output
                .clone()
                .unwrap_or_else(|| file.with_extension("geocoded.csv"));
            eprintln!(
                "warning: `mater geocode-csv` is deprecated; use `voidline places geocode-csv --file {} --column {} --output {}` instead",
                file.display(),
                column,
                output.display()
            );
            geocode_csv(file, column, &output).await?;
            println!("wrote geocoded CSV to {}", output.display());
            return Ok(());
        }
        Command::FixPlaceCitiesTypos => {
            fix_places_cities_typos()?;
            return Ok(());
        }
        Command::GeocodePlaceCities => {
            eprintln!(
                "warning: `mater geocode-place-cities` is deprecated; use `voidline places geocode-place-cities` instead"
            );
            geocode_places_cities().await?;
            return Ok(());
        }
        Command::NormalizePlaceCities => {
            eprintln!(
                "warning: `mater normalize-place-cities` is deprecated; use `voidline places normalize-place-cities` instead"
            );
            normalize_places_cities()?;
            return Ok(());
        }
        _ => {}
    }

    let database_url = format!("sqlite://{}", cli.database.display());
    let pool = connect(&database_url)
        .await
        .with_context(|| format!("failed to connect to {}", cli.database.display()))?;
    migrate(&pool).await?;

    match cli.command {
        Command::Status { date } => {
            let for_date = match date.as_deref() {
                Some(value) => Some(
                    parse_yyyy_mm_dd(value).ok_or_else(|| anyhow!("invalid date: {}", value))?,
                ),
                None => None,
            };

            let data = load_data(&pool).await?;
            let state = compute_state(&data, for_date);
            println!("total acquired: {:.2} g", state.total_acquired);
            println!("remaining (from log): {:.2} g", state.remaining);
            if let (Some(d), Some(rem)) = (for_date, state.remaining_at_date) {
                println!("remaining on {}: {:.2} g", d.format("%Y-%m-%d"), rem);
            }
            println!("tare weight: {:.2} g", state.tare);
            println!("calculated current weight: {:.2} g", state.current_weight);
        }
        Command::Add { amount, unit } => {
            let entry = add_usage(&pool, amount, &unit).await?;
            println!("added usage entry: {:?}", entry);

            let data = load_data(&pool).await?;
            let state = compute_state(&data, None);
            println!("new remaining (from log): {:.2} g", state.remaining);
        }
        Command::Backup { output } => {
            backup_possessions(&pool, &output).await?;
            println!("written backup to {}", output.display());
        }
        Command::FormatDate { .. }
        | Command::ConvertCsv { .. }
        | Command::UnifyCsv { .. }
        | Command::Geocode { .. }
        | Command::GeocodeCsv { .. }
        | Command::FixPlaceCitiesTypos
        | Command::GeocodePlaceCities
        | Command::NormalizePlaceCities => unreachable!(),
    }

    Ok(())
}

fn normalize_date_value(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        return value.to_string();
    }

    // Handle ranges like "April 1, 2024 → April 5, 2024".
    if value.contains('→') {
        let parts: Vec<_> = value.split('→').map(|p| p.trim()).collect();
        let normalized: Vec<String> = parts
            .into_iter()
            .map(|part| {
                parse_flexible_date(part)
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| part.to_string())
            })
            .collect();
        return normalized.join(" → ");
    }

    parse_flexible_date(value)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| value.to_string())
}

fn format_date(file: &PathBuf, columns: &[String]) -> Result<()> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(file)
        .with_context(|| format!("failed to open {}", file.display()))?;

    let headers = reader
        .headers()
        .with_context(|| format!("failed to read headers from {}", file.display()))?
        .clone();

    let mut col_indices = Vec::new();
    for col in columns {
        if let Some(idx) = headers.iter().position(|h| h == col) {
            col_indices.push(idx);
        } else {
            return Err(anyhow!("column not found: {}", col));
        }
    }

    let tmp_path = file.with_extension("tmp");
    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(&tmp_path)
        .with_context(|| format!("failed to create temp file {}", tmp_path.display()))?;

    writer.write_record(&headers)?;
    for result in reader.records() {
        let record = result.with_context(|| format!("failed to read record from {}", file.display()))?;
        let mut normalized: Vec<String> = record.iter().map(|v| v.to_string()).collect();
        for &idx in &col_indices {
            if let Some(val) = normalized.get(idx) {
                normalized[idx] = normalize_date_value(val);
            }
        }
        writer.write_record(&normalized)?;
    }
    writer.flush()?;

    // Replace original file with normalized file (keeping existing permissions).
    let backup_path = file.with_extension("bak");
    if backup_path.exists() {
        fs::remove_file(&backup_path)
            .with_context(|| format!("failed to remove existing backup {}", backup_path.display()))?;
    }
    fs::rename(file, &backup_path)
        .with_context(|| format!("failed to backup {}", file.display()))?;

    if file.exists() {
        fs::remove_file(file)
            .with_context(|| format!("failed to remove original file {}", file.display()))?;
    }
    fs::rename(&tmp_path, file)
        .with_context(|| format!("failed to write normalized file to {}", file.display()))?;

    Ok(())
}
