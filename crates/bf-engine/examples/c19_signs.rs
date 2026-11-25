//! Round C19 kill test: can staged RaBitQ sign bits be compressed losslessly?
//! Usage: c19_signs URL NAMESPACE LEAVES_PER_SLAB MAX_SLABS (read-only ranged GETs; no writes).
//! Samples leaves from distinct slabs (segments), un-transposes each leaf's fastscan code block
//! into per-row sign bytes, and reports bytes per row for several lossless schemes plus the
//! order-0 per-dimension entropy bound and a decode-speed estimate.
use bf_storage::{Storage, StorageConfig};
use bf_vector::Directory;
use bf_wal::Wal;
use std::{error::Error, time::Instant};

const BLOCK: usize = 32; // fastscan rows per transposed block
const BLK: usize = 16384;

#[derive(Default, Clone)]
struct Acc {
    bytes: f64,
    ns: f64,
}

fn z(level: i32, data: &[u8]) -> Vec<u8> {
    zstd::bulk::compress(data, level).unwrap()
}

/// Transpose rows x width bytes into bit planes: plane d holds bit d of each row.
fn bitplanes(rows: &[u8], width: usize) -> Vec<u8> {
    let n = rows.len() / width;
    let nb = n.div_ceil(8);
    let mut out = vec![0u8; width * 8 * nb];
    for r in 0..n {
        for d in 0..width * 8 {
            if rows[r * width + d / 8] >> (d % 8) & 1 != 0 {
                out[d * nb + r / 8] |= 1 << (r % 8);
            }
        }
    }
    out
}
fn unplanes(planes: &[u8], width: usize, n: usize) -> Vec<u8> {
    let nb = n.div_ceil(8);
    let mut out = vec![0u8; n * width];
    for d in 0..width * 8 {
        for r in 0..n {
            if planes[d * nb + r / 8] >> (r % 8) & 1 != 0 {
                out[r * width + d / 8] |= 1 << (d % 8);
            }
        }
    }
    out
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let a: Vec<_> = std::env::args().collect();
    if a.len() != 5 {
        return Err("expected URL NAMESPACE LEAVES_PER_SLAB MAX_SLABS".into());
    }
    let per_slab: usize = a[3].parse()?;
    let max_slabs: usize = a[4].parse()?;
    let store = Storage::open(&StorageConfig {
        url: a[1].clone(),
        region: Some(std::env::var("AWS_REGION").unwrap_or("us-west-2".into())),
        profile: std::env::var("AWS_PROFILE").ok(),
        read_only: true,
        ..Default::default()
    })
    .await?;
    let manifest = Wal::new(store.clone(), &a[2])?
        .latest_manifest()
        .await?
        .ok_or("missing manifest")?
        .1;
    let tiers = manifest.ann_tiers.get("vector").ok_or("missing tier")?;
    let tier = tiers.iter().max_by_key(|t| t.rows()).ok_or("no tier")?;
    eprintln!("tier rows {} slabs {}", tier.rows(), tier.slabs.len());
    let dir = Directory::decode(&store.get_range(&tier.dir.key, 0..tier.dir.size).await?)?;
    let padded = dir.dims().next_power_of_two();
    let width = padded / 8;
    eprintln!("entries {} padded {padded}", dir.entries().len());
    // Pick leaves: spread over segments, evenly within each, rows >= 64.
    let mut by_seg: std::collections::BTreeMap<usize, Vec<usize>> = Default::default();
    for (i, e) in dir.entries().iter().enumerate() {
        if e.rows >= 64 {
            by_seg.entry(e.segment).or_default().push(i);
        }
    }
    eprintln!("segments with leaves {}", by_seg.len());
    let step = (by_seg.len() / max_slabs).max(1);
    let mut picks = vec![];
    for (_, leaves) in by_seg.iter().step_by(step).take(max_slabs) {
        let s = (leaves.len() / per_slab).max(1);
        picks.extend(leaves.iter().step_by(s).take(per_slab).copied());
    }
    let names = [
        "raw",
        "zstd1",
        "zstd3",
        "xor_prev_z1",
        "xor_prev_z3",
        "xor_major_z1",
        "xor_major_z3",
        "planes_z1",
        "planes_z3",
        "stored_layout_z3",
    ];
    let mut acc = vec![Acc::default(); names.len()];
    let (mut rows_total, mut entropy_bits, mut leaves_done) = (0usize, 0f64, 0usize);
    let mut segs = std::collections::BTreeSet::new();
    for &p in &picks {
        let e = &dir.entries()[p];
        let s = &dir.segments()[e.segment];
        let key = if s.codes_in_fragment { &s.fragment } else { &s.key };
        let blob = store.get_range(key, e.codes_range()).await?;
        let n = e.rows as usize;
        let stored = &blob[16 * n..];
        if stored.len() != width * BLOCK * n.div_ceil(BLOCK) {
            eprintln!("skip leaf {p}: unexpected code length");
            continue;
        }
        // Un-transpose to row-major sign bytes.
        let mut rows = vec![0u8; n * width];
        for r in 0..n {
            let (b, j) = (r / BLOCK, r % BLOCK);
            for t in 0..width {
                rows[r * width + t] = stored[b * width * BLOCK + t * BLOCK + j];
            }
        }
        segs.insert(e.segment);
        leaves_done += 1;
        rows_total += n;
        // Order-0 per-dimension entropy bound.
        for d in 0..padded {
            let ones = (0..n).filter(|r| rows[r * width + d / 8] >> (d % 8) & 1 != 0).count() as f64;
            let p = ones / n as f64;
            if p > 0. && p < 1. {
                entropy_bits += n as f64 * -(p * p.log2() + (1. - p) * (1. - p).log2());
            }
        }
        // Majority pattern.
        let mut maj = vec![0u8; width];
        for d in 0..padded {
            let ones = (0..n).filter(|r| rows[r * width + d / 8] >> (d % 8) & 1 != 0).count();
            if ones * 2 > n {
                maj[d / 8] |= 1 << (d % 8);
            }
        }
        let xp: Vec<u8> = (0..rows.len())
            .map(|i| if i < width { rows[i] } else { rows[i] ^ rows[i - width] })
            .collect();
        let xm: Vec<u8> = rows.iter().enumerate().map(|(i, b)| b ^ maj[i % width]).collect();
        let rpb = BLK / width; // rows per 16 KiB block
        acc[0].bytes += rows.len() as f64;
        for lv in [1, 3] {
            let i_plain = if lv == 1 { 1 } else { 2 };
            // per scheme: (index, data builder per block)
            for (idx, data) in [
                (i_plain, &rows),
                (if lv == 1 { 3 } else { 4 }, &xp),
                (if lv == 1 { 5 } else { 6 }, &xm),
            ] {
                for (bi, chunk) in data.chunks(rpb * width).enumerate() {
                    let mut c = chunk.to_vec();
                    if idx == 3 || idx == 4 {
                        // restart prediction chain at each block so blocks decode independently
                        for i in (width..c.len()).rev() {
                            c[i] = rows[bi * rpb * width + i] ^ rows[bi * rpb * width + i - width];
                        }
                        for i in 0..width.min(c.len()) {
                            c[i] = rows[bi * rpb * width + i];
                        }
                    }
                    let t = Instant::now();
                    let out = z(lv, &c);
                    let _ = t;
                    acc[idx].bytes += out.len() as f64;
                    let t = Instant::now();
                    let back = zstd::bulk::decompress(&out, c.len() + 16).unwrap();
                    // inverse transform cost
                    let mut sink = 0u8;
                    match idx {
                        3 | 4 => {
                            let mut b = back.clone();
                            for i in width..b.len() {
                                b[i] ^= b[i - width];
                            }
                            sink ^= b[b.len() - 1];
                        }
                        5 | 6 => {
                            let mut b = back.clone();
                            for (i, x) in b.iter_mut().enumerate() {
                                *x ^= maj[i % width];
                            }
                            sink ^= b[b.len() - 1];
                        }
                        _ => sink ^= back[back.len() - 1],
                    }
                    std::hint::black_box(sink);
                    acc[idx].ns += t.elapsed().as_nanos() as f64;
                }
            }
            // bit planes per block
            for chunk in rows.chunks(rpb * width) {
                let pl = bitplanes(chunk, width);
                let out = z(lv, &pl);
                let idx = if lv == 1 { 7 } else { 8 };
                acc[idx].bytes += out.len() as f64;
                let t = Instant::now();
                let back = zstd::bulk::decompress(&out, pl.len() + 16).unwrap();
                let un = unplanes(&back, width, chunk.len() / width);
                std::hint::black_box(&un);
                acc[idx].ns += t.elapsed().as_nanos() as f64;
            }
        }
        // As stored (fastscan layout), 16 KiB blocks, zstd3.
        for chunk in stored.chunks(BLK) {
            acc[9].bytes += z(3, chunk).len() as f64;
        }
    }
    let r = rows_total as f64;
    let mut res = serde_json::Map::new();
    for (n, a) in names.iter().zip(&acc) {
        res.insert(
            n.to_string(),
            serde_json::json!({"bytes_per_row": a.bytes / r, "decode_ns_per_row": a.ns / r}),
        );
    }
    println!(
        "{}",
        serde_json::json!({"slabs": segs.len(), "leaves": leaves_done, "rows": rows_total,
        "width_bytes": width, "order0_entropy_bytes_per_row": entropy_bits / 8. / r, "schemes": res})
    );
    Ok(())
}
