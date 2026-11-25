//! Lossless f16 packing of dense vectors in large WAL payloads (WAL encoding 2).
//!
//! The engine stores the vectors of an f16 attribute in its WAL documents as base64 of f32
//! values that are already rounded to f16, so every value is exactly representable in half
//! the bytes. Writing those two bytes per value instead of four halves the JSON that is
//! compressed and uploaded, and leaves little for zstd to do. A payload is packed only when
//! every f16-attribute value in it round-trips bit for bit; otherwise it keeps encoding 1.
//! Decoding expands the packed strings back to the exact strings that were encoded.
use crate::{Error, WalEntry, WriteBatch};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bf_types::{AttributeType, DistanceMetric, Document, Id, Schema};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, SerializeStruct, Serializer};
use serde_json::value::RawValue;

/// The attributes of `batch` whose values are f16-rounded dense vectors.
fn f16_attributes(schema: &Schema) -> Vec<&str> {
    schema
        .iter()
        .filter(|(_, a)| matches!(a.data_type, Some(AttributeType::Vector { f16: true, .. })))
        .map(|(name, _)| name.as_str())
        .collect()
}

/// Pack one internal f32 base64 vector into f16 base64, or `None` if any value is not
/// exactly an f16 (or the string is not canonical base64 of whole f32 values).
fn pack(value: &str) -> Option<Box<RawValue>> {
    let bytes = STANDARD.decode(value).ok()?;
    if bytes.is_empty() || bytes.len() % 4 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(4) {
        let x = f32::from_le_bytes(chunk.try_into().unwrap());
        let h = half::f16::from_f32(x);
        if h.to_f32().to_bits() != x.to_bits() {
            return None;
        }
        out.extend_from_slice(&h.to_le_bytes());
    }
    // Base64 never needs JSON escaping, so the string is written out as is.
    RawValue::from_string(format!("\"{}\"", STANDARD.encode(out))).ok()
}

/// The inverse of [`pack`].
fn unpack(value: &str) -> Result<String, Error> {
    let bytes = STANDARD.decode(value).map_err(|_| Error::Format)?;
    if bytes.is_empty() || bytes.len() % 2 != 0 {
        return Err(Error::Format);
    }
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for chunk in bytes.chunks_exact(2) {
        let h = half::f16::from_le_bytes(chunk.try_into().unwrap());
        out.extend_from_slice(&h.to_f32().to_le_bytes());
    }
    Ok(STANDARD.encode(out))
}

/// Total bytes of f16-attribute strings, which cannot be inline when this exceeds the slot limit.
pub(crate) fn candidate_bytes(entry: &WalEntry) -> usize {
    entry
        .batches
        .iter()
        .map(|b| {
            let attrs = f16_attributes(&b.schema);
            b.upserts
                .iter()
                .flat_map(|d| attrs.iter().filter_map(|a| d.get(*a)))
                .filter_map(|v| v.as_str().map(str::len))
                .sum::<usize>()
        })
        .sum()
}

/// An entry's packed vectors, in document and attribute order, one list per batch.
pub(crate) struct Packed<'a> {
    entry: &'a WalEntry,
    attrs: Vec<Vec<&'a str>>,
    strings: Vec<Vec<Box<RawValue>>>,
}

/// Pack every f16-attribute vector in `entry`; `None` when there are none or any one
/// cannot be packed exactly, in which case the entry keeps the unpacked encoding.
pub(crate) fn pack_entry(entry: &WalEntry) -> Option<Packed<'_>> {
    let mut attrs = Vec::with_capacity(entry.batches.len());
    let mut strings = Vec::with_capacity(entry.batches.len());
    let mut any = false;
    for batch in &entry.batches {
        let names = f16_attributes(&batch.schema);
        let mut packed = Vec::new();
        // Visit values in document order, which is the order `Upserts` serializes them in.
        for doc in &batch.upserts {
            for (key, value) in doc {
                if !names.contains(&key.as_str()) {
                    continue;
                }
                match value {
                    serde_json::Value::Null => {}
                    serde_json::Value::String(s) => {
                        packed.push(pack(s)?);
                        any = true;
                    }
                    _ => return None,
                }
            }
        }
        attrs.push(names);
        strings.push(packed);
    }
    any.then_some(Packed {
        entry,
        attrs,
        strings,
    })
}

impl Serialize for Packed<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("WalEntry", 4)?;
        st.serialize_field("write_id", &self.entry.write_id)?;
        st.serialize_field("committed_at", &self.entry.committed_at)?;
        st.serialize_field("batches", &Batches(self))?;
        st.serialize_field("tombstone", &self.entry.tombstone)?;
        st.end()
    }
}
struct Batches<'a, 'b>(&'b Packed<'a>);
impl Serialize for Batches<'_, '_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.entry.batches.len()))?;
        for (i, batch) in self.0.entry.batches.iter().enumerate() {
            seq.serialize_element(&PackedBatch {
                batch,
                attrs: &self.0.attrs[i],
                strings: &self.0.strings[i],
            })?;
        }
        seq.end()
    }
}
struct PackedBatch<'a> {
    batch: &'a WriteBatch,
    attrs: &'a [&'a str],
    strings: &'a [Box<RawValue>],
}
impl Serialize for PackedBatch<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let b = self.batch;
        let mut st = s.serialize_struct("WriteBatch", 6)?;
        st.serialize_field(
            "upserts",
            &Upserts {
                docs: &b.upserts,
                attrs: self.attrs,
                strings: self.strings,
            },
        )?;
        st.serialize_field::<Vec<Id>>("deletes", &b.deletes)?;
        st.serialize_field::<Schema>("schema", &b.schema)?;
        st.serialize_field::<DistanceMetric>("distance_metric", &b.distance_metric)?;
        st.serialize_field("created_at", &b.created_at)?;
        st.serialize_field("updated_at", &b.updated_at)?;
        st.end()
    }
}
struct Upserts<'a> {
    docs: &'a [Document],
    attrs: &'a [&'a str],
    strings: &'a [Box<RawValue>],
}
impl Serialize for Upserts<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.docs.len()))?;
        let mut next = self.strings.iter();
        for doc in self.docs {
            seq.serialize_element(&PackedDoc {
                doc,
                attrs: self.attrs,
                next: std::cell::RefCell::new(&mut next),
            })?;
        }
        seq.end()
    }
}
struct PackedDoc<'a, 'b> {
    doc: &'a Document,
    attrs: &'a [&'a str],
    next: std::cell::RefCell<&'b mut std::slice::Iter<'a, Box<RawValue>>>,
}
impl Serialize for PackedDoc<'_, '_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.doc.len()))?;
        for (key, value) in self.doc {
            if self.attrs.contains(&key.as_str()) && value.is_string() {
                let packed = self.next.borrow_mut().next().ok_or_else(|| {
                    serde::ser::Error::custom("packed vector list is shorter than the documents")
                })?;
                map.serialize_entry(key, packed)?;
            } else {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

/// Expand packed vectors in a decoded encoding-2 entry back to internal f32 base64.
pub(crate) fn unpack_entry(entry: &mut WalEntry) -> Result<(), Error> {
    for batch in &mut entry.batches {
        let names: Vec<String> = f16_attributes(&batch.schema)
            .into_iter()
            .map(str::to_owned)
            .collect();
        for doc in &mut batch.upserts {
            for name in &names {
                match doc.get_mut(name) {
                    None | Some(serde_json::Value::Null) => {}
                    Some(serde_json::Value::String(s)) => *s = unpack(s)?,
                    Some(_) => return Err(Error::Format),
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FORMAT_VERSION, PACKED_FORMAT_VERSION, WriteBatch};
    use serde_json::{Value, json};

    fn f32_base64(values: &[f32]) -> String {
        STANDARD.encode(
            values
                .iter()
                .flat_map(|x| x.to_le_bytes())
                .collect::<Vec<_>>(),
        )
    }
    fn schema(f16: bool) -> Schema {
        let mut s = Schema::new();
        for (name, f16) in [("v", f16), ("w", true)] {
            let ty = if f16 { "[4]f16" } else { "[4]f32" };
            s.insert(
                name.into(),
                serde_json::from_value(json!({ "type": ty })).unwrap(),
            );
        }
        s
    }
    fn entry(rows: usize, vector: impl Fn(usize) -> Vec<f32>, f16: bool) -> WalEntry {
        let upserts = (0..rows)
            .map(|i| {
                let mut doc = Document::new();
                doc.insert("id".into(), json!(i));
                doc.insert("v".into(), Value::String(f32_base64(&vector(i))));
                doc.insert("w".into(), Value::String(f32_base64(&vector(i + 1))));
                doc.insert("text".into(), json!(format!("row {i}")));
                if i % 7 == 0 {
                    doc.insert("w".into(), Value::Null);
                }
                doc
            })
            .collect();
        WalEntry::new(vec![WriteBatch {
            upserts,
            deletes: vec![],
            schema: schema(f16),
            distance_metric: DistanceMetric::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }])
    }
    fn f16_values(i: usize) -> Vec<f32> {
        (0..4)
            .map(|d| half::f16::from_f32(((i * 31 + d * 7) as f32).sin() * 3.7).to_f32())
            .collect()
    }
    // 2,000 rows of two vectors exceed the inline limit.
    const ROWS: usize = 2000;

    #[test]
    fn large_f16_entries_pack_and_round_trip_exactly() {
        let original = entry(ROWS, f16_values, true);
        let bytes = original.encode().unwrap();
        assert_eq!(bytes[0], PACKED_FORMAT_VERSION);
        let back = WalEntry::decode(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(&back).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
        // The packed JSON is smaller than the plain one.
        let plain = serde_json::to_vec(&original).unwrap().len();
        let (packed, _) = original.to_json().unwrap();
        assert!(packed.len() < plain * 3 / 4, "{} vs {plain}", packed.len());
    }

    #[test]
    fn entries_that_do_not_pack_exactly_keep_encoding_one() {
        // One value that is not an f16 forces the plain encoding for the whole entry.
        let inexact = entry(ROWS, |i| vec![i as f32 + 0.1, 1., 2., 3.], true);
        let bytes = inexact.encode().unwrap();
        assert_eq!(bytes[0], FORMAT_VERSION);
        assert_eq!(
            serde_json::to_value(WalEntry::decode(&bytes).unwrap()).unwrap(),
            serde_json::to_value(&inexact).unwrap()
        );
        // A vector attribute that is f32 is never packed, and neither are small entries.
        assert_eq!(entry(ROWS, f16_values, false).encode().unwrap()[0], 1);
        assert_eq!(
            entry(3, f16_values, true).encode().unwrap()[0],
            FORMAT_VERSION
        );
        // Signed zero, subnormal f16 values, and the extremes round-trip; NaN falls back.
        let edge = |x: f32| entry(ROWS, move |_| vec![x, 0.0, -0.0, 65504.], true);
        for x in [5.960_464_5e-8, -65504., f32::INFINITY, f32::NAN] {
            let e = edge(x);
            let back = WalEntry::decode(&e.encode().unwrap()).unwrap();
            assert_eq!(
                serde_json::to_value(&back).unwrap(),
                serde_json::to_value(&e).unwrap()
            );
        }
    }

    #[test]
    fn encoding_one_payloads_still_decode() {
        let original = entry(ROWS, f16_values, true);
        let mut json = serde_json::to_vec(&original).unwrap();
        json.shrink_to_fit();
        let mut bytes = vec![FORMAT_VERSION];
        bytes.extend(zstd::stream::encode_all(json.as_slice(), 3).unwrap());
        assert_eq!(
            serde_json::to_value(WalEntry::decode(&bytes).unwrap()).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
    }

    #[test]
    fn corrupt_packed_payloads_fail_closed() {
        let e = entry(ROWS, f16_values, true);
        let (mut json, _) = e.to_json().unwrap();
        // Truncate the first packed vector to an odd byte count.
        let text = String::from_utf8(json.clone()).unwrap();
        let first = text.find("\"v\":\"").unwrap() + 5;
        let broken = format!("{}{}", &text[..first], &text[first + 4..]);
        json = broken.into_bytes();
        let mut bytes = vec![PACKED_FORMAT_VERSION];
        bytes.extend(zstd::stream::encode_all(json.as_slice(), 3).unwrap());
        assert!(WalEntry::decode(&bytes).is_err());
    }
}
