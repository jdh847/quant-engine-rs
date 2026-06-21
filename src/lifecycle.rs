use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleSummary {
    #[serde(default)]
    pub source_path: String,
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub event: String,
    #[serde(default)]
    pub tracked_orders: usize,
    #[serde(default)]
    pub created: usize,
    #[serde(default)]
    pub submitted: usize,
    #[serde(default)]
    pub acknowledged: usize,
    #[serde(default)]
    pub partial: usize,
    #[serde(default)]
    pub filled: usize,
    #[serde(default)]
    pub canceled: usize,
    #[serde(default)]
    pub rejected: usize,
    #[serde(default)]
    pub reconciled: usize,
    #[serde(default)]
    pub open: usize,
}

pub fn load_latest_lifecycle_summary(root: impl AsRef<Path>) -> Result<Option<LifecycleSummary>> {
    let Some(path) = first_existing_lifecycle_path(root.as_ref()) else {
        return Ok(None);
    };
    let text = fs::read_to_string(&path)
        .with_context(|| format!("read lifecycle log failed: {}", path.display()))?;

    let mut latest: Option<LifecycleSummary> = None;
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(summary) = parse_summary_line(&path, &value) {
            latest = Some(summary);
        }
    }

    Ok(latest)
}

pub fn format_lifecycle_summary(summary: &LifecycleSummary) -> String {
    format!(
        "source={}\ndate={}\nevent={}\ntracked_orders={}\ncreated={}\nsubmitted={}\nacknowledged={}\npartial={}\nfilled={}\ncanceled={}\nrejected={}\nreconciled={}\nopen={}\n",
        summary.source_path,
        summary.date,
        summary.event,
        summary.tracked_orders,
        summary.created,
        summary.submitted,
        summary.acknowledged,
        summary.partial,
        summary.filled,
        summary.canceled,
        summary.rejected,
        summary.reconciled,
        summary.open
    )
}

pub fn first_existing_lifecycle_path(root: &Path) -> Option<PathBuf> {
    let mut candidates = vec![
        root.join("ibkr_lifecycle_events.jsonl"),
        root.join("ibkr_lifecycle_summary.txt"),
        root.join("outputs_rust")
            .join("ibkr_lifecycle_events.jsonl"),
        root.join("outputs_rust").join("ibkr_lifecycle_summary.txt"),
    ];
    if let Some(parent) = root.parent() {
        candidates.push(parent.join("ibkr_lifecycle_events.jsonl"));
        candidates.push(parent.join("ibkr_lifecycle_summary.txt"));
        candidates.push(
            parent
                .join("outputs_rust")
                .join("ibkr_lifecycle_events.jsonl"),
        );
        candidates.push(
            parent
                .join("outputs_rust")
                .join("ibkr_lifecycle_summary.txt"),
        );
    }
    candidates.into_iter().find(|path| path.exists())
}

fn parse_summary_line(path: &Path, value: &Value) -> Option<LifecycleSummary> {
    let obj = value.as_object()?;
    if !obj.contains_key("tracked_orders") {
        return None;
    }
    Some(LifecycleSummary {
        source_path: path.display().to_string(),
        date: field_string(obj.get("date")),
        event: field_string(obj.get("event")),
        tracked_orders: field_usize(obj.get("tracked_orders")),
        created: field_usize(obj.get("created")),
        submitted: field_usize(obj.get("submitted")),
        acknowledged: field_usize(obj.get("acknowledged")),
        partial: field_usize(obj.get("partial")),
        filled: field_usize(obj.get("filled")),
        canceled: field_usize(obj.get("canceled")),
        rejected: field_usize(obj.get("rejected")),
        reconciled: field_usize(obj.get("reconciled")),
        open: field_usize(obj.get("open")),
    })
}

fn field_string(value: Option<&Value>) -> String {
    value
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default()
}

fn field_usize(value: Option<&Value>) -> usize {
    value
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{format_lifecycle_summary, load_latest_lifecycle_summary, LifecycleSummary};

    #[test]
    fn lifecycle_summary_parser_reads_latest_summary_line() {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("pqbot_lifecycle_{seed}"));
        fs::create_dir_all(&dir).expect("create");
        let path = dir.join("ibkr_lifecycle_events.jsonl");
        fs::write(
            &path,
            r#"{"date":"2026-04-04","event":"created","local_order_id":1}
{"date":"2026-04-04","event":"end_of_day","tracked_orders":2,"created":1,"submitted":1,"acknowledged":1,"partial":0,"filled":1,"canceled":0,"rejected":0,"reconciled":0,"open":0}
"#,
        )
        .expect("write");
        let parsed = load_latest_lifecycle_summary(&dir)
            .expect("load")
            .expect("some");
        assert_eq!(parsed.event, "end_of_day");
        assert_eq!(parsed.tracked_orders, 2);
        assert!(format_lifecycle_summary(&parsed).contains("open=0"));
    }

    #[test]
    fn lifecycle_summary_formatter_is_stable() {
        let summary = LifecycleSummary {
            source_path: "x".to_string(),
            date: "2026-04-04".to_string(),
            event: "reconcile".to_string(),
            tracked_orders: 1,
            created: 1,
            submitted: 1,
            acknowledged: 1,
            partial: 0,
            filled: 1,
            canceled: 0,
            rejected: 0,
            reconciled: 1,
            open: 0,
        };
        let text = format_lifecycle_summary(&summary);
        assert!(text.contains("event=reconcile"));
        assert!(text.contains("reconciled=1"));
    }
}
