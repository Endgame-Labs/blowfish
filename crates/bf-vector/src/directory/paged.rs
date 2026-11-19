//! A paged namespace directory: a coarse level a query reads first, and entry pages it reads only
//! for the coarse cells it probes.
//!
//! A flat [`Directory`] lists every leaf cluster, so its size grows with the namespace: at 100M rows
//! it is about 100 MB, and a cold query reads all of it to rank clusters (and pays the same in CPU
//! when warm). Here the entries are grouped into small **cells** of a couple of dozen neighboring
//! clusters, and one object holds:
//!
//! - a resident **header**: the segment table, each segment's centroid (needed to decode its one-bit
//!   codes), one one-bit code per cell (the cell's mean center, coded like a cluster's center, so a
//!   cell costs about as much as one entry), and a table of each cell's page range; then
//! - one **page** per cell: the cell's entries (the same fields a flat directory keeps) and their
//!   one-bit center codes.
//!
//! Cells are found by a two-level k-means over the clusters' approximate centers and laid out in
//! tour order, so the cells a query probes are close together in the object and their reads merge.
//! A query ranks the cells from the header, reads the pages of the nearest cells until they hold
//! several times as many clusters as it will probe, and [`PagedDirectory::assemble`]s those pages
//! into an ordinary [`Directory`] holding only those entries, which the existing ranking and scan
//! code consume unchanged.
use std::{ops::Range, sync::Arc};

use rayon::prelude::*;

use super::{
    DirEntry, DirSegment, Directory, ENTRY_BYTES, Parser, RaBitQ, Result, Section, Shape, bad,
    bit_bytes, check_crc, get_shape, prepare, put_shape, put_str, put_u32, put_u64,
    section_distances, text,
};
use crate::cluster;

const PAGED_MAGIC: &[u8; 8] = b"BFDIR007";
/// Bytes of one page entry: the segment index, then the flat entry's fields.
const PAGE_ENTRY_BYTES: usize = 4 + ENTRY_BYTES;
/// Entries per top-level group the cells are carved from.
const GROUP_ENTRIES: usize = 1000;

/// Per-segment data a query needs to decode that segment's entries.
#[derive(Debug)]
struct Head {
    shape: Shape,
    clusters: usize,
    rows: u32,
    membership: Range<u64>,
    codes_in_fragment: bool,
    centroid: Vec<f32>,
    /// Rows and entries the segment contributes in all (for sizing the probe plan).
    entry_rows: u64,
    entry_count: u64,
}

/// Where a cell's page sits in the object (relative to the end of the header).
#[derive(Clone, Copy, Debug)]
struct Cell {
    offset: u64,
    len: u32,
    count: u32,
    /// Root mean square distance of the cell's entries' centers from the cell's center.
    radius: f32,
}

/// The header of a paged directory: everything resident, no entries.
pub struct PagedDirectory {
    shape: Shape,
    segments: Vec<DirSegment>,
    heads: Vec<Head>,
    padded: usize,
    /// The cells' centers as a section of one-bit codes (its entries carry only norm and alpha).
    coarse: Section,
    cells: Vec<Cell>,
    rq: RaBitQ,
    /// Bytes of the header (the entries' pages start right after).
    pub metadata_len: u64,
}

/// One cell's entries, decoded.
pub struct Page {
    entries: Vec<DirEntry>,
    bits: Vec<u8>,
}
impl Page {
    /// Approximate resident bytes.
    pub fn resident_bytes(&self) -> u64 {
        (self.entries.len() * 2 * std::mem::size_of::<DirEntry>() + self.bits.len()) as u64
    }
}

/// The approximate rotated center of entry `n` of `section`: the segment centroid plus the entry's
/// one-bit code scaled to the stored offset norm.
fn approx_center(section: &Section, n: usize, out: &mut Vec<f32>) {
    let padded = section.centroid.len();
    let width = bit_bytes(padded);
    let code = &section.bits[n * width..(n + 1) * width];
    let norm = section.entries[n].norm;
    let unit = norm / (padded as f32).sqrt();
    out.clear();
    out.extend((0..padded).map(|i| {
        let bit = (code[i / 8] >> (i % 8)) & 1;
        section.centroid[i] + if bit == 1 { unit } else { -unit }
    }));
}

fn sq(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Greedy nearest-neighbor order of `points`, starting at the first.
fn tour_order(points: &[Vec<f32>]) -> Vec<usize> {
    let mut left: Vec<usize> = (1..points.len()).collect();
    let mut order = vec![0usize];
    while !left.is_empty() {
        let last = &points[*order.last().expect("tour is not empty")];
        let (at, _) = left
            .iter()
            .enumerate()
            .map(|(i, &c)| (i, sq(&points[c], last)))
            .min_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)))
            .expect("left is not empty");
        order.push(left.swap_remove(at));
    }
    order
}

type Members = Vec<(usize, usize)>;

impl Directory {
    /// Rearrange this directory's entries into cells of about `per_cell` neighboring clusters, and
    /// write them as a paged object. Returns the object bytes and the header's length.
    /// Deterministic for a seed.
    ///
    /// `files` gives each segment's `(size, metadata length)` for its index object and its fragment.
    pub fn build_paged(
        &self,
        per_cell: usize,
        seed: u64,
        files: &[[(u64, u64); 2]],
    ) -> Result<(Vec<u8>, u64)> {
        if files.len() != self.sections.len() {
            return Err(bad("file sizes do not match the directory's segments"));
        }
        let padded = self.shape.dims.next_power_of_two();
        let per_cell = per_cell.max(1);
        // (section, entry within section) of every entry, in directory order.
        let flat: Vec<(usize, usize)> = self
            .sections
            .iter()
            .enumerate()
            .flat_map(|(s, sec)| (0..sec.entries.len()).map(move |n| (s, n)))
            .collect();
        if flat.is_empty() {
            return Err(bad("directory has no entries"));
        }
        // Level one: groups of about GROUP_ENTRIES neighboring entries.
        let groups_n = (flat.len() / GROUP_ENTRIES).max(1);
        let sample_n = (groups_n * 100).clamp(20_000, 100_000).min(flat.len());
        let step = flat.len() as f64 / sample_n as f64;
        let sample: Vec<Vec<f32>> = (0..sample_n)
            .into_par_iter()
            .map(|i| {
                let (s, n) = flat[(i as f64 * step) as usize];
                let mut v = Vec::new();
                approx_center(&self.sections[s], n, &mut v);
                v
            })
            .collect();
        let refs: Vec<&[f32]> = sample.iter().map(Vec::as_slice).collect();
        let top = cluster::kmeans(&refs, groups_n, 8, seed);
        drop(refs);
        drop(sample);
        let assigned: Vec<u32> = flat
            .par_iter()
            .map_init(Vec::new, |buf, &(s, n)| {
                approx_center(&self.sections[s], n, buf);
                cluster::nearest(buf, &top) as u32
            })
            .collect();
        let mut groups: Vec<Members> = vec![Vec::new(); top.len()];
        for (&m, &g) in flat.iter().zip(&assigned) {
            groups[g as usize].push(m);
        }
        let kept: Vec<usize> = (0..groups.len())
            .filter(|g| !groups[*g].is_empty())
            .collect();
        let group_order: Vec<usize> = {
            let centers: Vec<Vec<f32>> = kept.iter().map(|g| top[*g].clone()).collect();
            tour_order(&centers).into_iter().map(|i| kept[i]).collect()
        };
        // Level two: cells inside each group, ordered along a tour.
        let per_group: Vec<Vec<(Vec<f32>, Members)>> = group_order
            .par_iter()
            .map(|&g| {
                let members = &groups[g];
                let points: Vec<Vec<f32>> = members
                    .iter()
                    .map(|&(s, n)| {
                        let mut v = Vec::new();
                        approx_center(&self.sections[s], n, &mut v);
                        v
                    })
                    .collect();
                let k = members.len().div_ceil(per_cell);
                let centers = if k <= 1 {
                    vec![mean(&points, padded)]
                } else {
                    let refs: Vec<&[f32]> = points.iter().map(Vec::as_slice).collect();
                    cluster::kmeans(&refs, k, 6, seed ^ g as u64)
                };
                let mut cells: Vec<Members> = vec![Vec::new(); centers.len()];
                let mut cell_points: Vec<Vec<Vec<f32>>> = vec![Vec::new(); centers.len()];
                for (m, p) in members.iter().zip(points) {
                    let c = cluster::nearest(&p, &centers);
                    cells[c].push(*m);
                    cell_points[c].push(p);
                }
                let mut out: Vec<(Vec<f32>, Members)> = cells
                    .into_iter()
                    .zip(cell_points)
                    .filter(|(m, _)| !m.is_empty())
                    .map(|(m, p)| (mean(&p, padded), m))
                    .collect();
                let order = tour_order(&out.iter().map(|c| c.0.clone()).collect::<Vec<_>>());
                let mut slots: Vec<Option<(Vec<f32>, Members)>> = out.drain(..).map(Some).collect();
                order
                    .into_iter()
                    .map(|i| slots[i].take().expect("tour visits each cell once"))
                    .collect()
            })
            .collect();
        let mut cells: Vec<(Vec<f32>, Members)> = per_group.into_iter().flatten().collect();
        for (_, m) in &mut cells {
            m.sort_unstable_by_key(|&(s, n)| (s, self.sections[s].entries[n].cluster));
        }
        // The cells' centers, coded relative to their mean like a segment's clusters.
        let mut centroid = vec![0f32; padded];
        for (c, _) in &cells {
            for (g, x) in centroid.iter_mut().zip(c) {
                *g += *x;
            }
        }
        for g in &mut centroid {
            *g /= cells.len() as f32;
        }
        let width = bit_bytes(padded);
        let root = (padded as f64).sqrt();
        let mut coarse_bits = Vec::with_capacity(cells.len() * width);
        let mut coarse_entries = Vec::with_capacity(cells.len());
        let radii: Vec<f32> = cells
            .par_iter()
            .map(|(c, m)| {
                let mut buf = Vec::new();
                let total: f32 = m
                    .iter()
                    .map(|&(s, n)| {
                        approx_center(&self.sections[s], n, &mut buf);
                        sq(&buf, c)
                    })
                    .sum();
                (total / m.len().max(1) as f32).sqrt()
            })
            .collect();
        for (c, m) in &cells {
            let r: Vec<f64> = c
                .iter()
                .zip(&centroid)
                .map(|(x, g)| f64::from(*x - *g))
                .collect();
            let norm = r.iter().map(|x| x * x).sum::<f64>().sqrt();
            let mut code = vec![0u8; width];
            for (i, x) in r.iter().enumerate() {
                if *x >= 0. {
                    code[i / 8] |= 1 << (i % 8);
                }
            }
            coarse_bits.extend(code);
            let alpha = if norm == 0. {
                1.
            } else {
                r.iter().map(|x| x.abs()).sum::<f64>() / (norm * root)
            };
            coarse_entries.push((norm as f32, alpha.clamp(1e-6, 1.) as f32, m.len() as u32));
        }
        let mut pages: Vec<Vec<u8>> = Vec::with_capacity(cells.len());
        for (_, m) in &cells {
            let mut page = Vec::with_capacity(4 + m.len() * (PAGE_ENTRY_BYTES + width) + 4);
            put_u32(&mut page, m.len() as u32);
            for &(s, n) in m {
                put_u32(&mut page, s as u32);
                put_entry(&mut page, &self.sections[s].entries[n]);
            }
            for &(s, n) in m {
                page.extend_from_slice(&self.sections[s].bits[n * width..(n + 1) * width]);
            }
            let crc = crc32fast::hash(&page);
            put_u32(&mut page, crc);
            pages.push(page);
        }
        let mut header = PAGED_MAGIC.to_vec();
        put_shape(&mut header, &self.shape);
        put_u32(&mut header, self.sections.len() as u32);
        for ((seg, sec), sizes) in self.segments.iter().zip(&self.sections).zip(files) {
            for (size, meta) in sizes {
                put_u64(&mut header, *size);
                put_u64(&mut header, *meta);
            }
            put_str(&mut header, &seg.id);
            put_str(&mut header, &seg.key);
            put_str(&mut header, &seg.fragment);
            put_u32(&mut header, u32::from(sec.shape.flat));
            put_u32(&mut header, sec.clusters as u32);
            put_u32(&mut header, sec.rows);
            put_u64(&mut header, sec.membership.start);
            put_u64(&mut header, sec.membership.end);
            put_u32(&mut header, u32::from(sec.codes_in_fragment));
            put_u64(
                &mut header,
                sec.entries.iter().map(|e| u64::from(e.rows)).sum(),
            );
            put_u64(&mut header, sec.entries.len() as u64);
            for x in &sec.centroid {
                header.extend(x.to_le_bytes());
            }
        }
        put_u32(&mut header, cells.len() as u32);
        for x in &centroid {
            header.extend(x.to_le_bytes());
        }
        let mut offset = 0u64;
        for (((norm, alpha, count), page), radius) in coarse_entries.iter().zip(&pages).zip(&radii)
        {
            header.extend(norm.to_le_bytes());
            header.extend(alpha.to_le_bytes());
            header.extend(radius.to_le_bytes());
            put_u32(&mut header, *count);
            put_u64(&mut header, offset);
            put_u32(&mut header, page.len() as u32);
            offset += page.len() as u64;
        }
        header.extend(&coarse_bits);
        let crc = crc32fast::hash(&header);
        put_u32(&mut header, crc);
        let header_len = header.len() as u64;
        let mut out = header;
        for page in pages {
            out.extend(page);
        }
        Ok((out, header_len))
    }
}

fn mean(points: &[Vec<f32>], padded: usize) -> Vec<f32> {
    let mut m = vec![0f32; padded];
    for p in points {
        for (a, b) in m.iter_mut().zip(p) {
            *a += *b;
        }
    }
    for a in &mut m {
        *a /= points.len().max(1) as f32;
    }
    m
}

fn put_entry(out: &mut Vec<u8>, e: &DirEntry) {
    put_u32(out, e.cluster as u32);
    put_u32(out, e.rows);
    for v in [e.codes.start, e.codes.end, e.vectors.start, e.vectors.end] {
        put_u64(out, v);
    }
    put_u32(out, e.codes_crc);
    put_u32(out, e.vectors_crc);
    put_u64(out, e.blob.start);
    put_u32(out, (e.blob.end - e.blob.start) as u32);
    out.extend(e.norm.to_le_bytes());
    out.extend(e.alpha.to_le_bytes());
}

impl PagedDirectory {
    /// Whether `bytes` start a paged directory (rather than a flat one).
    pub fn is_paged(bytes: &[u8]) -> bool {
        bytes.starts_with(PAGED_MAGIC)
    }

    /// Decode a header (the first `metadata_len` bytes of the object).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(bad("truncated paged directory"));
        }
        let (body, tail) = bytes.split_at(bytes.len() - 4);
        check_crc(body, u32::from_le_bytes(tail.try_into().unwrap()))?;
        let mut p = Parser::new(body);
        if p.take(8)? != PAGED_MAGIC {
            return Err(bad("bad paged directory magic"));
        }
        let shape = get_shape(&mut p)?;
        let padded = shape.dims.next_power_of_two();
        let nsegs = p.u32()? as usize;
        if nsegs > p.remaining() {
            return Err(bad("invalid paged directory header"));
        }
        let mut segments = Vec::with_capacity(nsegs);
        let mut heads = Vec::with_capacity(nsegs);
        for _ in 0..nsegs {
            let sizes = [(p.u64()?, p.u64()?), (p.u64()?, p.u64()?)];
            let id = text(&mut p)?;
            let key = text(&mut p)?;
            let fragment = text(&mut p)?;
            let flat = p.u32()? != 0;
            let clusters = p.u32()? as usize;
            let rows = p.u32()?;
            let membership = p.u64()?..p.u64()?;
            let codes_in_fragment = p.u32()? != 0;
            let entry_rows = p.u64()?;
            let entry_count = p.u64()?;
            let centroid = p.vector(padded)?;
            if membership.start > membership.end {
                return Err(bad("invalid paged directory segment"));
            }
            segments.push(DirSegment {
                id,
                key,
                fragment,
                flat,
                clusters,
                rows: rows as usize,
                membership: (!membership.is_empty()).then(|| membership.clone()),
                codes_in_fragment,
                files: Some(sizes),
            });
            heads.push(Head {
                shape: Shape { flat, ..shape },
                clusters,
                rows,
                membership,
                codes_in_fragment,
                centroid,
                entry_rows,
                entry_count,
            });
        }
        let ncells = p.u32()? as usize;
        if ncells == 0 || ncells > p.remaining() {
            return Err(bad("invalid paged directory cells"));
        }
        let centroid = p.vector(padded)?;
        let mut cells = Vec::with_capacity(ncells);
        let mut entries = Vec::with_capacity(ncells);
        for _ in 0..ncells {
            let norm = f32::from_le_bytes(p.take(4)?.try_into().unwrap());
            let alpha = f32::from_le_bytes(p.take(4)?.try_into().unwrap());
            let radius = f32::from_le_bytes(p.take(4)?.try_into().unwrap());
            let count = p.u32()?;
            let offset = p.u64()?;
            let len = p.u32()?;
            if !norm.is_finite() || !radius.is_finite() || !(alpha > 0. && alpha <= 1.) {
                return Err(bad("invalid paged directory cell"));
            }
            cells.push(Cell {
                offset,
                len,
                count,
                radius,
            });
            entries.push(DirEntry {
                segment: 0,
                cluster: 0,
                rows: count,
                codes: 0..0,
                vectors: 0..0,
                codes_crc: 0,
                vectors_crc: 0,
                blob: 0..0,
                norm,
                alpha,
            });
        }
        let bits = p.take(ncells * bit_bytes(padded))?.to_vec();
        if p.remaining() != 0 {
            return Err(bad("inconsistent paged directory length"));
        }
        let coarse = Section {
            shape,
            clusters: ncells,
            centroid,
            entries,
            bits,
            rows: 0,
            membership: 0..0,
            codes_in_fragment: false,
        };
        Ok(Self {
            shape: Shape {
                flat: false,
                ..shape
            },
            segments,
            heads,
            padded,
            coarse,
            cells,
            rq: RaBitQ::new(shape.dims, shape.seed)?,
            metadata_len: bytes.len() as u64,
        })
    }

    /// Segments in the order entries refer to them.
    pub fn segments(&self) -> &[DirSegment] {
        &self.segments
    }
    /// Rows and clusters the pages list in all.
    pub fn totals(&self) -> (u64, u64) {
        (
            self.heads.iter().map(|h| h.entry_rows).sum(),
            self.heads.iter().map(|h| h.entry_count).sum(),
        )
    }
    /// Size of the whole object, from the header's page table.
    pub fn object_len(&self) -> u64 {
        self.metadata_len
            + self
                .cells
                .iter()
                .map(|c| c.offset + u64::from(c.len))
                .max()
                .unwrap_or(0)
    }
    /// Number of cells (pages).
    pub fn cell_count(&self) -> usize {
        self.cells.len()
    }
    /// Entries in all pages together.
    pub fn entry_count(&self) -> u64 {
        self.heads.iter().map(|h| h.entry_count).sum()
    }
    /// Approximate resident bytes.
    pub fn resident_bytes(&self) -> u64 {
        (self.coarse.bits.len()
            + self.coarse.entries.len() * std::mem::size_of::<DirEntry>()
            + self
                .heads
                .iter()
                .map(|h| h.centroid.len() * 4)
                .sum::<usize>()
            + self.segments.len() * 320) as u64
    }
    /// Byte range of a cell's page in the whole object.
    pub fn page_range(&self, cell: usize) -> Range<u64> {
        let c = &self.cells[cell];
        let start = self.metadata_len + c.offset;
        start..start + u64::from(c.len)
    }

    /// The cells to read for `query`: the nearest by estimated distance (see `kappa`) until together they
    /// list at least `want` entries (all cells when the directory is smaller). Sorted by cell, so
    /// that neighboring pages can be read together.
    pub fn select_cells(&self, query: &[f32], want: usize, kappa: f32) -> Result<Vec<usize>> {
        crate::distance::validate(query, self.shape.dims)?;
        let q = self.rq.rotate(&prepare(query, self.shape.metric));
        // A cell's nearest entry is nearer than its center by about its spread: rank by the
        // distance a typical nearest entry would have, d^2 + r^2 - kappa * d * r.
        let d: Vec<f32> = section_distances(&self.coarse, &q)
            .into_iter()
            .zip(&self.cells)
            .map(|(d2, c)| d2 + c.radius * c.radius - kappa * d2.max(0.).sqrt() * c.radius)
            .collect();
        let mut order: Vec<usize> = (0..d.len()).collect();
        order.sort_unstable_by(|a, b| d[*a].total_cmp(&d[*b]).then(a.cmp(b)));
        let (mut got, mut picked) = (0usize, Vec::new());
        for i in order {
            picked.push(i);
            got += self.cells[i].count as usize;
            if got >= want {
                break;
            }
        }
        picked.sort_unstable();
        Ok(picked)
    }

    /// Decode one page's bytes (the range from [`PagedDirectory::page_range`]).
    pub fn decode_page(&self, bytes: &[u8]) -> Result<Page> {
        if bytes.len() < 8 {
            return Err(bad("truncated directory page"));
        }
        let (body, tail) = bytes.split_at(bytes.len() - 4);
        check_crc(body, u32::from_le_bytes(tail.try_into().unwrap()))?;
        let mut p = Parser::new(body);
        let n = p.u32()? as usize;
        let width = bit_bytes(self.padded);
        if n > p.remaining() || p.remaining() != n * (PAGE_ENTRY_BYTES + width) {
            return Err(bad("invalid directory page length"));
        }
        let entries = (0..n)
            .map(|_| {
                let segment = p.u32()? as usize;
                let cluster = p.u32()? as usize;
                let rows = p.u32()?;
                let codes = p.u64()?..p.u64()?;
                let vectors = p.u64()?..p.u64()?;
                let codes_crc = p.u32()?;
                let vectors_crc = p.u32()?;
                let start = p.u64()?;
                let blob = start..start + u64::from(p.u32()?);
                let norm = f32::from_le_bytes(p.take(4)?.try_into().unwrap());
                let alpha = f32::from_le_bytes(p.take(4)?.try_into().unwrap());
                let head = self
                    .heads
                    .get(segment)
                    .ok_or_else(|| bad("page entry outside directory"))?;
                if cluster >= head.clusters || !norm.is_finite() || !(alpha > 0. && alpha <= 1.) {
                    return Err(bad("page entry outside segment"));
                }
                Ok(DirEntry {
                    segment,
                    cluster,
                    rows,
                    codes,
                    vectors,
                    codes_crc,
                    vectors_crc,
                    blob,
                    norm,
                    alpha,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let bits = p.take(n * width)?.to_vec();
        Ok(Page { entries, bits })
    }

    /// An ordinary directory over the entries of `pages` only. It lists every segment and reports
    /// the whole namespace's totals through [`Directory::segment_totals`], so the probe plan is
    /// sized from the full corpus rather than from the pages read.
    pub fn assemble(&self, pages: &[Arc<Page>]) -> Directory {
        let width = bit_bytes(self.padded);
        let mut entries: Vec<Vec<DirEntry>> = vec![Vec::new(); self.heads.len()];
        let mut bits: Vec<Vec<u8>> = vec![Vec::new(); self.heads.len()];
        for page in pages {
            for (n, e) in page.entries.iter().enumerate() {
                entries[e.segment].push(e.clone());
                bits[e.segment].extend_from_slice(&page.bits[n * width..(n + 1) * width]);
            }
        }
        let mut sections = Vec::with_capacity(self.heads.len());
        let mut all = Vec::new();
        for (head, (es, bs)) in self.heads.iter().zip(entries.into_iter().zip(bits)) {
            all.extend(es.iter().cloned());
            sections.push(Arc::new(Section {
                shape: head.shape,
                clusters: head.clusters,
                centroid: head.centroid.clone(),
                entries: es,
                bits: bs,
                rows: head.rows,
                membership: head.membership.clone(),
                codes_in_fragment: head.codes_in_fragment,
            }));
        }
        Directory {
            shape: self.shape,
            segments: self.segments.clone(),
            sections,
            entries: all,
            rq: self.rq.clone(),
            metadata_len: self.metadata_len,
            totals: Some(
                self.heads
                    .iter()
                    .map(|h| (h.entry_rows as usize, h.entry_count as usize))
                    .collect(),
            ),
        }
    }
}
