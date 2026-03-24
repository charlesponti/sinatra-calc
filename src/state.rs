use chrono::NaiveDate;
use regex::Regex;

use crate::model::SubstanceData;

/// Computed totals for a given dataset.
pub struct StateResult {
    pub total_acquired: f64,
    pub remaining: f64,
    pub tare: f64,
    pub current_weight: f64,
    pub remaining_at_date: Option<f64>,
}

pub fn parse_yyyy_mm_dd(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

/// Try parsing a date from common human-readable formats.
///
/// This is used by `mater format-date` to convert dates like "April 8, 2017"
/// into `YYYY-MM-DD` before rewriting the CSV.
pub fn parse_flexible_date(s: &str) -> Option<NaiveDate> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%B %e, %Y", // "April 8, 2017"
        "%b %e, %Y", // "Apr 8, 2017"
        "%B %d, %Y", // "April 08, 2017"
        "%b %d, %Y", // "Apr 08, 2017"
    ];

    for fmt in formats {
        if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
            return Some(d);
        }
    }

    None
}

pub fn compute_state(data: &SubstanceData, for_date: Option<NaiveDate>) -> StateResult {
    let total_acquired = data.acquisition.iter().map(|a| a.value_g).sum::<f64>();

    let mut remaining = total_acquired;
    let mut remaining_at_date = None;

    for entry in &data.usage_log {
        if entry.r#type == "pattern" {
            if let (Some(start), Some(end)) = (
                entry.start_date.as_ref().and_then(|s| parse_yyyy_mm_dd(s)),
                entry.end_date.as_ref().and_then(|s| parse_yyyy_mm_dd(s)),
            ) {
                let diff = (end - start).num_days() + 1;
                remaining -= diff as f64 * entry.amount;
            }
        } else if entry.r#type == "event" {
            remaining -= entry.amount;
        }
    }

    if let Some(date) = for_date {
        let mut rem = total_acquired;
        for entry in &data.usage_log {
            if entry.r#type == "pattern" {
                if let (Some(start), Some(end)) = (
                    entry.start_date.as_ref().and_then(|s| parse_yyyy_mm_dd(s)),
                    entry.end_date.as_ref().and_then(|s| parse_yyyy_mm_dd(s)),
                ) {
                    if date < start {
                        continue;
                    }
                    let last = if date < end { date } else { end };
                    let diff = (last - start).num_days() + 1;
                    rem -= diff as f64 * entry.amount;
                }
            } else if entry.r#type == "event" {
                if let Some(ts) = entry.timestamp.as_ref().and_then(|t| parse_yyyy_mm_dd(t)) {
                    if ts <= date {
                        rem -= entry.amount;
                    }
                }
            }
        }
        remaining_at_date = Some(rem);
    }

    let tare = data
        .containers
        .iter()
        .find(|c| {
            if c.id == "glass" {
                return true;
            }
            if let Some(label) = &c.label {
                Regex::new("(?i)glass").unwrap().is_match(label)
            } else {
                false
            }
        })
        .map(|c| c.tare_weight_g)
        .unwrap_or(0.0);

    let current_weight = tare + remaining;

    StateResult {
        total_acquired,
        remaining,
        tare,
        current_weight,
        remaining_at_date,
    }
}
