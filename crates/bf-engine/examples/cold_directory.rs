//! Export immutable leaf ranges and current one-bit routing scores for offline kill tests.
//!
//! Usage: cold_directory directory.bin queries.f32 output_directory [paged.bin header_bytes]
//! Queries are consecutive little-endian f32 rows with the directory's dimensions.
//! Outputs must live under `.scratch/`, and must be removed after measurement.
use std::{error::Error, io::Write, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    export(&args)
}

fn export(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() != 3 && args.len() != 5 {
        return Err(
            "expected directory.bin queries.f32 output_directory [paged.bin header_bytes]".into(),
        );
    }
    let out = PathBuf::from(&args[2]);
    if !out.components().any(|p| p.as_os_str() == ".scratch") {
        return Err("output must be under .scratch/".into());
    }
    std::fs::create_dir_all(&out)?;
    let out = out.canonicalize()?;
    if !out.components().any(|p| p.as_os_str() == ".scratch") {
        return Err("output must be under .scratch/".into());
    }
    let dir = bf_vector::upgrade::directory(&std::fs::read(&args[0])?)?;
    let queries = std::fs::read(&args[1])?;
    let width = dir.dims() * 4;
    if queries.is_empty() || !queries.len().is_multiple_of(width) {
        return Err("queries must contain whole, nonempty f32 rows".into());
    }
    let entries: Vec<_> = dir
        .entries()
        .iter()
        .map(|e| {
            let s = &dir.segments()[e.segment];
            serde_json::json!({"segment":e.segment,"cluster":e.cluster,"rows":e.rows,
            "key":s.key,"fragment":s.fragment,"codes_in_fragment":s.codes_in_fragment,
            "blob":[e.blob_range().start,e.blob_range().end],
            "codes":[e.codes_range().start,e.codes_range().end],
            "vectors":[e.vectors_range().start,e.vectors_range().end]})
        })
        .collect();
    std::fs::write(out.join("entries.json"), serde_json::to_vec(&entries)?)?;
    let paged = if args.len() == 5 {
        let raw = std::fs::read(&args[3])?;
        let header: usize = args[4].parse()?;
        let pg =
            bf_vector::PagedDirectory::decode(raw.get(..header).ok_or("header outside object")?)?;
        let positions: std::collections::BTreeMap<_, _> = dir
            .entries()
            .iter()
            .enumerate()
            .map(|(n, e)| ((e.segment, e.cluster), n))
            .collect();
        let mut cells = Vec::new();
        for c in 0..pg.cell_count() {
            let r = pg.page_range(c);
            let page = std::sync::Arc::new(
                pg.decode_page(
                    raw.get(r.start as usize..r.end as usize)
                        .ok_or("page outside object")?,
                )?,
            );
            let view = pg.assemble(&[page]);
            cells.push(
                view.entries()
                    .iter()
                    .map(|e| {
                        positions
                            .get(&(e.segment, e.cluster))
                            .copied()
                            .ok_or("paged directory differs from flat directory")
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        std::fs::write(out.join("cells.json"), serde_json::to_vec(&cells)?)?;
        Some(pg)
    } else {
        None
    };
    let mut selected =
        std::io::BufWriter::new(std::fs::File::create(out.join("page-selection.jsonl"))?);
    let mut scores = std::io::BufWriter::new(std::fs::File::create(out.join("onebit.f32"))?);
    for raw in queries.chunks_exact(width) {
        let q: Vec<_> = raw
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect();
        for d in dir.center_distances(&q)? {
            scores.write_all(&d.to_le_bytes())?;
        }
        if let Some(pg) = &paged {
            let (rows, leaves) = pg.totals();
            let probes =
                bf_vector::tier_probe_budget(64, rows as usize, leaves as usize).ceil() as usize;
            let picks: Vec<_> = [2, 3, 4, 8, 16]
                .into_iter()
                .map(|margin| pg.select_cells(&q, probes * margin, 0.5))
                .collect::<Result<_, _>>()?;
            writeln!(selected, "{}", serde_json::to_string(&picks)?)?;
        }
    }
    scores.flush()?;
    selected.flush()?;
    println!(
        "{}",
        serde_json::json!({"entries":entries.len(),"queries":queries.len()/width,
        "dims":dir.dims(),"vector_row_bytes":dir.vector_row_bytes()})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bf_vector::{BuildParams, Directory, Fragment, IndexBuilder, Metric};

    #[test]
    fn exported_pages_partition_the_flat_entries_and_preserve_routing_scores() {
        let scratch = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
        std::fs::create_dir_all(&scratch).unwrap();
        let work = tempfile::Builder::new()
            .prefix("cold-directory-")
            .tempdir_in(scratch)
            .unwrap();
        let rows: Vec<_> = (0..128)
            .map(|i| (i, vec![i as f32, (i % 7) as f32, 1., -2.]))
            .collect();
        let blob = IndexBuilder::new(4, Metric::EuclideanSquared)
            .unwrap()
            .params(BuildParams {
                flat_threshold: 0,
                clusters: Some(8),
                ..Default::default()
            })
            .build(&rows)
            .unwrap();
        let (ann, len) = Fragment::build_ann(&blob).unwrap();
        let fragment = Fragment::decode(&ann[..len as usize]).unwrap();
        let raw = Directory::build(&[("s".into(), "index".into(), "index".into(), fragment)])
            .unwrap()
            .unwrap();
        let dir = Directory::decode(&raw).unwrap();
        let (paged, header) = dir
            .build_paged(2, 42, &[[(ann.len() as u64, len); 2]])
            .unwrap();
        let input = work.path().join("directory");
        let pages = work.path().join("paged");
        let queries = work.path().join("queries");
        let out = work.path().join("out");
        std::fs::write(&input, raw).unwrap();
        std::fs::write(&pages, paged).unwrap();
        let q = [3f32, 2., 1., -1.];
        std::fs::write(
            &queries,
            q.iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<_>>(),
        )
        .unwrap();
        export(&[
            input.display().to_string(),
            queries.display().to_string(),
            out.display().to_string(),
            pages.display().to_string(),
            header.to_string(),
        ])
        .unwrap();
        let scores = std::fs::read(out.join("onebit.f32")).unwrap();
        let actual: Vec<_> = scores
            .as_chunks::<4>()
            .0
            .iter()
            .map(|v| f32::from_le_bytes(*v))
            .collect();
        assert_eq!(actual, dir.center_distances(&q).unwrap());
        let groups: Vec<Vec<usize>> =
            serde_json::from_slice(&std::fs::read(out.join("cells.json")).unwrap()).unwrap();
        let mut indices: Vec<_> = groups.into_iter().flatten().collect();
        indices.sort_unstable();
        assert_eq!(indices, (0..dir.entries().len()).collect::<Vec<_>>());
        // An incomplete query row must fail rather than silently shifting the scores.
        std::fs::write(&queries, [0u8; 15]).unwrap();
        assert!(
            export(&[
                input.display().to_string(),
                queries.display().to_string(),
                out.display().to_string()
            ])
            .is_err()
        );
    }
}
