use std::{fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use serde::Deserialize;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "geo - geolocation lookup and CSV geocoding",
    long_about = "A CLI for looking up place coordinates via OpenStreetMap Nominatim and enriching CSV files with geocoding data.",
    arg_required_else_help = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Lookup a place name using OSM Nominatim and print coordinates.
    Geocode {
        /// The query to look up (e.g. "Mahopac, New York").
        query: String,
    },
    /// Geocode a CSV column (adds lat/lon/city/state/country/country_code columns).
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
        .header("User-Agent", "geo-cli/1.0")
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

    match cli.command {
        Command::Geocode { query } => {
            match geocode_nominatim(&query).await? {
                Some(place) => {
                    println!("{}", place.display_name);
                    println!("lat={}, lon={}", place.lat, place.lon);
                }
                None => println!("no results for '{}'", query),
            }
        }
        Command::GeocodeCsv { file, column, output } => {
            let output = output
                .unwrap_or_else(|| file.with_extension("geocoded.csv"));
            geocode_csv(&file, &column, &output).await?;
            println!("wrote geocoded CSV to {}", output.display());
        }
        Command::FixPlaceCitiesTypos => {
            fix_places_cities_typos()?;
        }
        Command::GeocodePlaceCities => {
            geocode_places_cities().await?;
        }
        Command::NormalizePlaceCities => {
            normalize_places_cities()?;
        }
    }

    Ok(())
}
