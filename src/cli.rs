use std::{path::PathBuf, time::Duration};

use anyhow::{anyhow, Context, Result};
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
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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
    fn best_city(&self) -> Option<&str> {
        self.city
            .as_deref()
            .or(self.town.as_deref())
            .or(self.village.as_deref())
            .or(self.hamlet.as_deref())
    }
}

fn nominatim_base_url() -> String {
    std::env::var("NOMINATIM_BASE_URL")
        .unwrap_or_else(|_| "https://nominatim.openstreetmap.org".to_string())
}

async fn geocode_nominatim(query: &str) -> Result<Option<NominatimResult>> {
    let base_url = nominatim_base_url();
    let url = format!(
        "{}/search?format=json&limit=1&addressdetails=1&q={}",
        base_url,
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
        let record =
            result.with_context(|| format!("failed to read record from {}", file.display()))?;
        let mut out_record: Vec<String> = record.iter().map(|v| v.to_string()).collect();

        if out_record.len() < out_headers.len() {
            out_record.resize(out_headers.len(), String::new());
        }

        let query = out_record.get(col_index).map(|s| s.trim()).unwrap_or("");

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
            tokio::time::sleep(Duration::from_millis(1100)).await;
            if let Some(place) = geocode_nominatim(query).await? {
                let lat = place.lat.clone();
                let lon = place.lon.clone();
                let city = place
                    .address
                    .as_ref()
                    .and_then(|a| a.best_city())
                    .map(str::to_owned)
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

/// Runs the CLI and dispatches to the selected subcommand.
pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Geocode { query } => match geocode_nominatim(&query).await? {
            Some(place) => {
                println!("{}", place.display_name);
                println!("lat={}, lon={}", place.lat, place.lon);
            }
            None => println!("no results for '{}'", query),
        },
        Command::GeocodeCsv {
            file,
            column,
            output,
        } => {
            let output = output.unwrap_or_else(|| file.with_extension("geocoded.csv"));
            geocode_csv(&file, &column, &output).await?;
            println!("wrote geocoded CSV to {}", output.display());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn best_city_prefers_city_then_fallbacks() {
        let address = NominatimAddress {
            city: None,
            town: Some("Townsville".to_string()),
            village: Some("Village".to_string()),
            hamlet: Some("Hamlet".to_string()),
            state: None,
            country: None,
            country_code: None,
        };

        assert_eq!(address.best_city(), Some("Townsville"));
    }

    #[test]
    fn best_city_returns_empty_for_nil_address() {
        let address: NominatimAddress = NominatimAddress {
            city: None,
            town: None,
            village: None,
            hamlet: None,
            state: None,
            country: None,
            country_code: None,
        };

        assert_eq!(address.best_city(), None);
    }
}
