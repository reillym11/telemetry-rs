//! Parquet output on an object store (`OUTPUT_PATH`), exactly once together with the Kafka
//! transaction. See docs/DESIGN.md, Parquet sink.
//!
//! Rows encoded in a transaction are buffered, then written as one file per partition directory
//! just before the transaction's offsets are sent. A file is named after the committed input
//! offset total when its transaction started, which is unique per committed transaction because
//! every transaction consumes at least one record. On startup, files at or above the committed
//! total belong to transactions that never committed and are deleted; on abort the open
//! transaction's files are deleted. Single replica only: the total spans every input partition.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow_array::builder::{
    Decimal128Builder, Int32Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use tokio::runtime::Handle;

/// Physical column types, matching what Spark writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Utf8,
    /// RFC 3339 string in the encoded row; INT64 TIMESTAMP(MILLIS, UTC) in the file.
    TimestampMillis,
    Int32,
    /// Decimal number or string in the encoded row; more fractional digits than `scale` is an error.
    Decimal {
        precision: u8,
        scale: i8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: &'static str,
    pub ty: ColumnType,
    pub nullable: bool,
}

/// The file layout of a Parquet job. `partition_by` names a string field of the encoded row that
/// becomes the `<name>=<value>/` directory and is not written as a column, as Spark's `partitionBy`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetSpec {
    pub columns: Vec<Column>,
    pub partition_by: &'static str,
}

pub(crate) struct ParquetSink {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    spec: ParquetSpec,
    rows: Vec<Value>,
    uploaded: Vec<Path>,
}

/// `s3a://bucket/some/prefix` -> (bucket, prefix).
fn parse_url(url: &str) -> Result<(String, String)> {
    let rest = ["s3a://", "s3://"]
        .iter()
        .find_map(|s| url.strip_prefix(s))
        .with_context(|| "OUTPUT_PATH must be an s3a:// or s3:// URL")?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        bail!("OUTPUT_PATH has no bucket");
    }
    Ok((bucket.to_string(), prefix.trim_end_matches('/').to_string()))
}

fn file_name(start_total: i64) -> String {
    format!("part-{start_total:020}.parquet")
}

/// The start total of a file this sink wrote, from its name.
fn start_total(path: &Path) -> Option<i64> {
    path.filename()?
        .strip_prefix("part-")?
        .strip_suffix(".parquet")?
        .parse()
        .ok()
}

impl ParquetSink {
    pub(crate) fn new(url: &str, endpoint: Option<&str>, spec: ParquetSpec) -> Result<Self> {
        let (bucket, prefix) = parse_url(url)?;
        let mut b = AmazonS3Builder::from_env().with_bucket_name(bucket);
        if std::env::var_os("AWS_REGION").is_none()
            && std::env::var_os("AWS_DEFAULT_REGION").is_none()
        {
            b = b.with_region("us-east-1");
        }
        if let Some(e) = endpoint {
            b = b.with_endpoint(e).with_allow_http(e.starts_with("http://"));
        }
        let store = b.build().context("create object store client")?;
        Ok(ParquetSink {
            store: Arc::new(store),
            prefix: Path::from(prefix),
            spec,
            rows: Vec::new(),
            uploaded: Vec::new(),
        })
    }

    pub(crate) fn push(&mut self, value: &[u8]) -> Result<()> {
        let v: Value = serde_json::from_slice(value).context("encoded row is not JSON")?;
        if !v.is_object() {
            bail!("encoded row is not a JSON object");
        }
        self.rows.push(v);
        Ok(())
    }

    /// Write the open transaction's rows, one file per partition directory, in row order.
    pub(crate) fn write(&mut self, start_total: i64, rt: &Handle) -> Result<()> {
        let mut groups: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for r in self.rows.drain(..) {
            let part = r
                .get(self.spec.partition_by)
                .and_then(Value::as_str)
                .with_context(|| format!("row has no string `{}`", self.spec.partition_by))?
                .to_string();
            if part.is_empty() || part.contains('/') {
                bail!("invalid `{}` value", self.spec.partition_by);
            }
            groups.entry(part).or_default().push(r);
        }
        for (part, rows) in groups {
            let bytes = encode(&self.spec, &rows)?;
            let path = self
                .prefix
                .clone()
                .join(format!("{}={part}", self.spec.partition_by))
                .join(file_name(start_total));
            self.uploaded.push(path.clone());
            rt.block_on(self.store.put(&path, PutPayload::from(bytes)))
                .context("upload parquet file")?;
        }
        Ok(())
    }

    /// The open transaction committed: its files stay.
    pub(crate) fn committed(&mut self) {
        self.rows.clear();
        self.uploaded.clear();
    }

    /// The open transaction aborted: drop its rows and delete the files it uploaded.
    pub(crate) fn abort(&mut self, rt: &Handle) -> Result<()> {
        self.rows.clear();
        for p in std::mem::take(&mut self.uploaded) {
            match rt.block_on(self.store.delete(&p)) {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(anyhow!(e).context("delete aborted parquet file")),
            }
        }
        Ok(())
    }

    /// Delete files written by transactions that started at or after the committed total, which
    /// therefore never committed. Returns how many were deleted.
    pub(crate) fn delete_uncommitted(&self, committed_total: i64, rt: &Handle) -> Result<usize> {
        let listed: Vec<_> = rt
            .block_on(self.store.list(Some(&self.prefix)).try_collect())
            .context("list parquet files")?;
        let mut deleted = 0;
        for m in listed {
            if start_total(&m.location).is_some_and(|s| s >= committed_total) {
                rt.block_on(self.store.delete(&m.location))
                    .context("delete uncommitted parquet file")?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

fn arrow_type(ty: ColumnType) -> DataType {
    match ty {
        ColumnType::Utf8 => DataType::Utf8,
        ColumnType::TimestampMillis => {
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        }
        ColumnType::Int32 => DataType::Int32,
        ColumnType::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
    }
}

/// Decimal text (`96.5`, `-0.25`) as an unscaled integer at `scale`. Exact; no rounding.
pub fn parse_decimal(text: &str, precision: u8, scale: i8) -> Result<i128> {
    let (neg, digits) = match text.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, text),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let frac = frac.trim_end_matches('0');
    if int.is_empty()
        || !int.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        bail!("not a plain decimal number");
    }
    if frac.len() > scale.max(0) as usize {
        bail!("decimal has more than {scale} fractional digits");
    }
    let mut unscaled: i128 = format!("{int}{frac:0<width$}", width = scale.max(0) as usize)
        .parse()
        .context("decimal out of range")?;
    if unscaled >= 10i128.pow(precision as u32) {
        bail!("decimal does not fit precision {precision}");
    }
    if neg {
        unscaled = -unscaled;
    }
    Ok(unscaled)
}

fn parse_ts(s: &str) -> Result<i64> {
    Ok(chrono::DateTime::parse_from_rfc3339(s)
        .context("timestamp is not RFC 3339")?
        .timestamp_millis())
}

/// Encode rows as one Snappy-compressed Parquet file.
pub fn encode(spec: &ParquetSpec, rows: &[Value]) -> Result<Vec<u8>> {
    let fields: Vec<Field> = spec
        .columns
        .iter()
        .map(|c| Field::new(c.name, arrow_type(c.ty), c.nullable))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for c in &spec.columns {
        let values = rows.iter().map(|r| r.get(c.name).filter(|v| !v.is_null()));
        if !c.nullable && values.clone().any(|v| v.is_none()) {
            bail!("column `{}` is null", c.name);
        }
        let err = || format!("column `{}` has a value of the wrong type", c.name);
        let array: ArrayRef = match c.ty {
            ColumnType::Utf8 => {
                let mut b = StringBuilder::new();
                for v in values {
                    b.append_option(v.map(|v| v.as_str().with_context(err)).transpose()?);
                }
                Arc::new(b.finish())
            }
            ColumnType::TimestampMillis => {
                let mut b = TimestampMillisecondBuilder::new().with_timezone("UTC");
                for v in values {
                    let ms = v
                        .map(|v| v.as_str().with_context(err).and_then(parse_ts))
                        .transpose()?;
                    b.append_option(ms);
                }
                Arc::new(b.finish())
            }
            ColumnType::Int32 => {
                let mut b = Int32Builder::new();
                for v in values {
                    let n = v
                        .map(|v| {
                            v.as_i64()
                                .and_then(|n| i32::try_from(n).ok())
                                .with_context(err)
                        })
                        .transpose()?;
                    b.append_option(n);
                }
                Arc::new(b.finish())
            }
            ColumnType::Decimal { precision, scale } => {
                let mut b = Decimal128Builder::new();
                for v in values {
                    let d = v
                        .map(|v| {
                            let text = match v {
                                Value::String(s) => s.clone(),
                                Value::Number(n) => n.to_string(),
                                _ => return Err(anyhow!(err())),
                            };
                            parse_decimal(&text, precision, scale).with_context(err)
                        })
                        .transpose()?;
                    b.append_option(d);
                }
                Arc::new(
                    b.finish()
                        .with_precision_and_scale(precision, scale)
                        .context("decimal precision")?,
                )
            }
        };
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays).context("build record batch")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).context("parquet writer")?;
    w.write(&batch).context("write parquet")?;
    w.close().context("close parquet")?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_output_path() {
        assert_eq!(
            parse_url("s3a://telemetry-lake/run/rust/battery_health_hourly/").unwrap(),
            (
                "telemetry-lake".into(),
                "run/rust/battery_health_hourly".into()
            )
        );
        assert!(parse_url("file:///tmp").is_err());
    }

    #[test]
    fn file_names_round_trip_their_start_total() {
        let p = Path::from("x/date=2026-10-01").join(file_name(21_806));
        assert_eq!(start_total(&p), Some(21_806));
        assert_eq!(start_total(&Path::from("x/_SUCCESS")), None);
    }

    #[test]
    fn decimals_are_exact() {
        assert_eq!(parse_decimal("96.5", 5, 2).unwrap(), 9650);
        assert_eq!(parse_decimal("-0.25", 5, 2).unwrap(), -25);
        assert_eq!(parse_decimal("25.70", 4, 1).unwrap(), 257);
        assert!(parse_decimal("96.495", 5, 2).is_err());
        assert!(parse_decimal("1000.0", 4, 1).is_err());
        assert!(parse_decimal("1e3", 5, 2).is_err());
    }
}
