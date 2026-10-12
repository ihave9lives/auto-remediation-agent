use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use config::{Config, File};
use dashmap::DashMap;
use prometheus_parse::{Scrape, Value};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "watchdog", version, about = "Anomaly detection daemon for telemetry streams")]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    #[arg(short, long)]
    dry_run: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct WatchdogConfig {
    prometheus: PrometheusConfig,
    clickhouse: ClickHouseConfig,
    detection: DetectionConfig,
    output: OutputConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct PrometheusConfig {
    url: String,
    scrape_interval_secs: u64,
    query_timeout_secs: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ClickHouseConfig {
    url: String,
    database: String,
    username: String,
    password: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct DetectionConfig {
    window_secs: u64,
    zscore_threshold: f64,
    ewma_alpha: f64,
    entropy_threshold: f64,
    min_samples: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct OutputConfig {
    clickhouse_table: String,
    webhook_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IncidentPayload {
    id: String,
    timestamp: chrono::DateTime<chrono::Utc>,
    service: String,
    anomaly_type: AnomalyType,
    severity: Severity,
    metrics: HashMap<String, f64>,
    context: IncidentContext,
    confidence: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
enum AnomalyType {
    ErrorRateSpike,
    LatencyDegradation,
    ThroughputDrop,
    LogEntropyBurst,
    ResourceExhaustion,
    CascadeFailure,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Severity {
    Info,
    Warning,
    Critical,
    Emergency,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IncidentContext {
    pod: Option<String>,
    namespace: Option<String>,
    deployment: Option<String>,
    recent_deploys: Vec<String>,
    related_services: Vec<String>,
}

#[derive(Debug, Clone)]
struct MetricSeries {
    name: String,
    labels: HashMap<String, String>,
    values: Vec<(f64, f64)>, // (timestamp, value)
    ewma: Option<f64>,
}

impl MetricSeries {
    fn new(name: String, labels: HashMap<String, String>) -> Self {
        Self {
            name,
            labels,
            values: Vec::new(),
            ewma: None,
        }
    }

    fn push(&mut self, timestamp: f64, value: f64, alpha: f64) {
        self.values.push((timestamp, value));
        self.ewma = match self.ewma {
            Some(prev) => Some(alpha * value + (1.0 - alpha) * prev),
            None => Some(value),
        };

        // Keep only window
        let cutoff = timestamp - 300.0; // 5 min max
        self.values.retain(|(ts, _)| *ts > cutoff);
    }

    fn zscore(&self) -> Option<f64> {
        if self.values.len() < 10 {
            return None;
        }
        let vals: Vec<f64> = self.values.iter().map(|(_, v)| *v).collect();
        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
        let variance = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64;
        let std = variance.sqrt();
        if std == 0.0 {
            return None;
        }
        let current = vals.last()?;
        Some((current - mean) / std)
    }

    fn ewma_deviation(&self) -> Option<f64> {
        let current = self.values.last()?.1;
        let ewma = self.ewma?;
        if ewma == 0.0 {
            return None;
        }
        Some((current - ewma).abs() / ewma)
    }
}

struct Watchdog {
    config: WatchdogConfig,
    client: reqwest::Client,
    series: Arc<DashMap<String, MetricSeries>>,
    incident_tx: mpsc::Sender<IncidentPayload>,
}

impl Watchdog {
    fn new(config: WatchdogConfig, incident_tx: mpsc::Sender<IncidentPayload>) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
            series: Arc::new(DashMap::new()),
            incident_tx,
        }
    }

    async fn run(&self) -> Result<()> {
        let mut scrape_interval = interval(Duration::from_secs(self.config.prometheus.scrape_interval_secs));
        let mut entropy_interval = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = scrape_interval.tick() => {
                    if let Err(e) = self.scrape_and_analyze().await {
                        error!("Scrape failed: {}", e);
                    }
                }
                _ = entropy_interval.tick() => {
                    if let Err(e) = self.analyze_log_entropy().await {
                        error!("Log entropy analysis failed: {}", e);
                    }
                }
            }
        }
    }

    async fn scrape_and_analyze(&self) -> Result<()> {
        let queries = vec![
            ("error_rate", "sum(rate(http_requests_total{status=~\"5..\"}[1m])) by (service, pod)"),
            ("latency_p99", "histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[1m])) by (le, service, pod))"),
            ("latency_p95", "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[1m])) by (le, service, pod))"),
            ("throughput", "sum(rate(http_requests_total[1m])) by (service, pod)"),
            ("cpu_usage", "sum(rate(container_cpu_usage_seconds_total[1m])) by (pod, namespace)"),
            ("memory_usage", "container_memory_usage_bytes / container_spec_memory_limit_bytes"),
        ];

        for (name, query) in queries {
            self.execute_query(name, query).await?;
        }

        self.detect_anomalies().await
    }

    async fn execute_query(&self, metric_name: &str, query: &str) -> Result<()> {
        let url = format!("{}/api/v1/query", self.config.prometheus.url);
        let resp = self.client
            .get(&url)
            .query(&[("query", query)])
            .timeout(Duration::from_secs(self.config.prometheus.query_timeout_secs))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        if let Some(data) = resp.get("data").and_then(|d| d.get("result")).and_then(|r| r.as_array()) {
            let now = chrono::Utc::now().timestamp() as f64;
            for sample in data {
                let mut labels = HashMap::new();
                if let Some(metric) = sample.get("metric").and_then(|m| m.as_object()) {
                    for (k, v) in metric {
                        if let Some(v_str) = v.as_str() {
                            labels.insert(k.clone(), v_str.to_string());
                        }
                    }
                }

                let value = sample.get("value").and_then(|v| v.as_array())
                    .and_then(|arr| arr.get(1))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok());

                if let Some(val) = value {
                    let service = labels.get("service").cloned().unwrap_or_else(|| "unknown".to_string());
                    let key = format!("{}:{}:{}", metric_name, service, labels.get("pod").cloned().unwrap_or_default());

                    let mut series = self.series.entry(key.clone()).or_insert_with(|| {
                        MetricSeries::new(metric_name.to_string(), labels.clone())
                    });
                    series.push(now, val, self.config.detection.ewma_alpha);
                }
            }
        }
        Ok(())
    }

    async fn detect_anomalies(&self) -> Result<()> {
        for entry in self.series.iter() {
            let series = entry.value();
            let labels: HashMap<String, String> = series.labels.clone();
            let service = labels.get("service").cloned().unwrap_or_else(|| "unknown".to_string());
            let pod = labels.get("pod").cloned();

            // Z-score detection
            if let Some(zscore) = series.zscore() {
                if zscore.abs() > self.config.detection.zscore_threshold {
                    let anomaly_type = match series.name.as_str() {
                        "error_rate" => AnomalyType::ErrorRateSpike,
                        "latency_p99" | "latency_p95" => AnomalyType::LatencyDegradation,
                        "throughput" => AnomalyType::ThroughputDrop,
                        "cpu_usage" | "memory_usage" => AnomalyType::ResourceExhaustion,
                        _ => AnomalyType::ErrorRateSpike,
                    };

                    let severity = if zscore.abs() > 5.0 { Severity::Emergency }
                        else if zscore.abs() > 3.5 { Severity::Critical }
                        else if zscore.abs() > 2.5 { Severity::Warning }
                        else { Severity::Info };

                    self.emit_incident(IncidentPayload {
                        id: Uuid::new_v4().to_string(),
                        timestamp: chrono::Utc::now(),
                        service: service.clone(),
                        anomaly_type,
                        severity,
                        metrics: self.collect_current_metrics(&service).await,
                        context: IncidentContext {
                            pod,
                            namespace: labels.get("namespace").cloned(),
                            deployment: labels.get("deployment").cloned(),
                            recent_deploys: self.fetch_recent_deploys(&service).await,
                            related_services: self.find_related_services(&service).await,
                        },
                        confidence: (zscore.abs() / 5.0).min(1.0),
                    }).await?;
                }
            }

            // EWMA deviation detection
            if let Some(dev) = series.ewma_deviation() {
                if dev > 0.5 { // 50% deviation from EWMA
                    // Similar incident emission...
                }
            }
        }
        Ok(())
    }

    async fn collect_current_metrics(&self, service: &str) -> HashMap<String, f64> {
        let mut metrics = HashMap::new();
        for entry in self.series.iter() {
            let series = entry.value();
            if series.labels.get("service") == Some(&service.to_string()) {
                if let Some((_, val)) = series.values.last() {
                    metrics.insert(series.name.clone(), *val);
                }
            }
        }
        metrics
    }

    async fn fetch_recent_deploys(&self, service: &str) -> Vec<String> {
        // Query git or ArgoCD for recent deployments
        vec![format!("{}-v1.2.3", service), format!("{}-v1.2.2", service)]
    }

    async fn find_related_services(&self, service: &str) -> Vec<String> {
        // From service mesh / trace data
        vec!["service-b".to_string(), "service-c".to_string()]
    }

    async fn analyze_log_entropy(&self) -> Result<()> {
        // Query Loki for recent logs, compute Shannon entropy over log templates
        // Using Drain-style clustering
        let url = format!("{}/loki/api/v1/query_range", self.config.clickhouse.url.replace("tcp://", "http://").replace(":9000", ":3100"));
        
        let resp = self.client
            .get(&url)
            .query(&[
                ("query", "{job=~\"mock-services.*\"}"),
                ("start", &(chrono::Utc::now() - chrono::Duration::minutes(5)).timestamp_millis().to_string()),
                ("end", &chrono::Utc::now().timestamp_millis().to_string()),
                ("limit", "10000"),
            ])
            .send()
            .await?;

        // Parse logs, extract templates, compute entropy
        // Simplified: emit incident if log volume spikes with high template diversity
        
        Ok(())
    }

    async fn emit_incident(&self, incident: IncidentPayload) -> Result<()> {
        info!(
            "ANOMALY DETECTED: {} | {} | {:?} | confidence: {:.2}",
            incident.service, incident.id, incident.anomaly_type, incident.confidence
        );

        // Write to ClickHouse
        self.write_to_clickhouse(&incident).await?;

        // Send to webhook if configured
        if let Some(webhook) = &self.config.output.webhook_url {
            self.client.post(webhook).json(&incident).send().await?;
        }

        // Send to agent via channel
        self.incident_tx.send(incident).await?;
        
        Ok(())
    }

    async fn write_to_clickhouse(&self, incident: &IncidentPayload) -> Result<()> {
        let url = format!("{}/", self.config.clickhouse.url);
        let query = format!(
            "INSERT INTO {}.{} (id, timestamp, service, anomaly_type, severity, metrics, context, confidence) VALUES",
            self.config.clickhouse.database, self.config.output.clickhouse_table
        );
        
        // Simplified - in production use proper ClickHouse client
        debug!("Would write to ClickHouse: {}", query);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    
    let config = Config::builder()
        .add_source(File::with_name(&args.config))
        .build()
        .context("Failed to load config")?
        .try_deserialize::<WatchdogConfig>()
        .context("Failed to deserialize config")?;

    let (incident_tx, mut incident_rx) = mpsc::channel(1000);

    let watchdog = Watchdog::new(config.clone(), incident_tx);
    
    // Spawn incident consumer (forwards to agent)
    let config_clone = config.clone();
    tokio::spawn(async move {
        while let Some(incident) = incident_rx.recv().await {
            // Forward to agent API
            let client = reqwest::Client::new();
            let _ = client
                .post("http://agent:8080/incidents")
                .json(&incident)
                .send()
                .await;
        }
    });

    info!("Watchdog started with config: {:?}", config);
    watchdog.run().await?;

    Ok(())
}