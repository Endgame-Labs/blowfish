//! A packet-center graph navigated by minimum residual fast-scan row score.
use super::{
    data::{Result, Vectors},
    fastscan::{self, FastQuery, RowCode},
    graph::{Graph, Hit, Search},
    ivf::Ivf,
    rabitq::RaBitQ,
};
use half::f16;
use rayon::prelude::*;
use std::{
    cell::Cell,
    collections::BinaryHeap,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    sync::Arc,
};
pub struct Packet {
    ids: Vec<u32>,
    center: Vec<f32>,
    codes: Vec<u8>,
}
pub struct Packets {
    packets: Vec<Packet>,
    dims: usize,
}
impl Packets {
    pub fn build(v: &Vectors, ivf: &Ivf, size: usize) -> Self {
        assert!(size == 16 || size == 32);
        let rq = RaBitQ::new(v.dims, 42).unwrap();
        let leaves = ivf.leaves();
        let packets: Vec<_> = leaves
            .into_par_iter()
            .flat_map_iter(|mut rows| {
                let mut out = Vec::new();
                while !rows.is_empty() {
                    let seed = rows[0];
                    rows.sort_by_cached_key(|id| (v.between(seed, *id).to_bits(), *id));
                    let ids: Vec<_> = rows.drain(..size.min(rows.len())).collect();
                    let mut center = vec![0f32; v.dims];
                    for &id in &ids {
                        for (c, x) in center.iter_mut().zip(v.normalized(id)) {
                            *c += x;
                        }
                    }
                    let center = super::data::normalized(&center);
                    let rotated = rq.rotate(&center);
                    let codes: Vec<_> = ids
                        .iter()
                        .map(|id| {
                            let code = rq.encode_unchecked(&v.normalized(*id), &center);
                            let cb = rotated
                                .iter()
                                .enumerate()
                                .map(|(i, x)| {
                                    if code.bits[i / 8] & (1 << (i % 8)) != 0 {
                                        *x
                                    } else {
                                        -*x
                                    }
                                })
                                .sum::<f64>()
                                / (rq.padded as f64).sqrt();
                            RowCode {
                                bits: code.bits,
                                norm: code.norm as f32,
                                alpha: code.alpha as f32,
                                cb: cb as f32,
                                zero: false,
                            }
                        })
                        .collect();
                    out.push(Packet {
                        ids,
                        center: rotated.iter().map(|x| *x as f32).collect(),
                        codes: fastscan::encode(&codes, rq.padded),
                    });
                }
                out
            })
            .collect();
        Self {
            packets,
            dims: rq.padded,
        }
    }
    pub fn centers(&self) -> Arc<Vectors> {
        // Orthogonal rotation preserves center-to-center cosine distances for construction.
        let values = self
            .packets
            .iter()
            .flat_map(|p| p.center.iter().map(|x| f16::from_f32(*x)))
            .collect();
        Arc::new(Vectors {
            dims: self.dims,
            values,
            inv_norms: vec![1.; self.packets.len()],
        })
    }
    pub fn len(&self) -> usize {
        self.packets.len()
    }
    pub fn write(&self, path: &Path) -> Result<()> {
        let mut f = BufWriter::new(File::create(path)?);
        f.write_all(b"G3PACK01")?;
        for n in [self.packets.len(), self.dims] {
            f.write_all(&(n as u32).to_le_bytes())?;
        }
        for p in &self.packets {
            f.write_all(&(p.ids.len() as u32).to_le_bytes())?;
            for id in &p.ids {
                f.write_all(&id.to_le_bytes())?;
            }
            for x in &p.center {
                f.write_all(&x.to_le_bytes())?;
            }
            f.write_all(&p.codes)?;
        }
        f.flush()?;
        Ok(())
    }
    pub fn read(path: &Path) -> Result<Self> {
        let mut f = BufReader::new(File::open(path)?);
        let mut magic = [0; 8];
        f.read_exact(&mut magic)?;
        if &magic != b"G3PACK01" {
            return Err("not G3 packets".into());
        }
        fn word(f: &mut impl Read) -> Result<u32> {
            let mut b = [0; 4];
            f.read_exact(&mut b)?;
            Ok(u32::from_le_bytes(b))
        }
        let n = word(&mut f)? as usize;
        let dims = word(&mut f)? as usize;
        if n == 0 || !(8..=65536).contains(&dims) || !dims.is_power_of_two() {
            return Err("invalid packet shape".into());
        }
        let mut packets = Vec::with_capacity(n);
        for _ in 0..n {
            let count = word(&mut f)? as usize;
            if !(1..=32).contains(&count) {
                return Err("invalid packet size".into());
            }
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(word(&mut f)?);
            }
            let mut center = Vec::with_capacity(dims);
            for _ in 0..dims {
                center.push(f32::from_bits(word(&mut f)?));
            }
            let mut codes = vec![0; fastscan::block_len(count, dims)];
            f.read_exact(&mut codes)?;
            fastscan::check_factors(&codes, count, dims)?;
            packets.push(Packet { ids, center, codes });
        }
        let mut b = [0];
        if f.read(&mut b)? != 0 {
            return Err("trailing packet bytes".into());
        }
        Ok(Self { packets, dims })
    }
    pub fn search(
        &self,
        g: &Graph,
        v: &Vectors,
        q: &[f32],
        ef: usize,
        rerank: usize,
        s: &mut PacketSearch,
    ) -> (Vec<Hit>, usize, usize) {
        s.reset();
        let rotated = RaBitQ::new(q.len(), 42).unwrap().rotate(q);
        let q32: Vec<_> = rotated.iter().map(|x| *x as f32).collect();
        let fq = FastQuery::new(&rotated, &vec![0.; self.dims]);
        let mut visited = 0;
        let mut rows = 0;
        let _ = g.search(ef, &mut s.graph, |id| {
            let id = id as usize;
            if s.marks[id] == s.generation {
                return s.distances[id];
            }
            visited += 1;
            s.marks[id] = s.generation;
            let p = &self.packets[id];
            rows += p.ids.len();
            let qc2 = bf_vector::squared_distance(&q32, &p.center);
            let mut min = f32::INFINITY;
            fq.scan(
                &p.codes,
                p.ids.len(),
                qc2,
                true,
                &Cell::new(f32::INFINITY),
                &mut |start, valid, dist, _, _| {
                    for j in 0..32 {
                        if valid & (1 << j) == 0 {
                            continue;
                        }
                        min = min.min(dist[j]);
                        let hit = Hit {
                            id: p.ids[start + j],
                            distance: dist[j],
                        };
                        if s.rows.len() < rerank {
                            s.rows.push(hit);
                        } else if hit < *s.rows.peek().unwrap() {
                            *s.rows.peek_mut().unwrap() = hit;
                        }
                    }
                },
            )
            .unwrap();
            s.distances[id] = min;
            min
        });
        let mut hits: Vec<_> = s
            .rows
            .iter()
            .map(|h| Hit {
                id: h.id,
                distance: v.rerank(q, h.id),
            })
            .collect();
        hits.sort_unstable();
        hits.truncate(10);
        (hits, visited, rows)
    }
}
pub struct PacketSearch {
    graph: Search,
    marks: Vec<u8>,
    generation: u8,
    distances: Vec<f32>,
    rows: BinaryHeap<Hit>,
}
impl PacketSearch {
    pub fn new(n: usize) -> Self {
        Self {
            graph: Search::new(n),
            marks: vec![0; n],
            generation: 0,
            distances: vec![0.; n],
            rows: BinaryHeap::new(),
        }
    }
    fn reset(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.marks.fill(0);
            self.generation = 1;
        }
        self.rows.clear();
    }
}
