// Disposable copy of search_internal, with bitmap row bounds before posting reads.
impl SegmentReader {
    #[allow(clippy::too_many_arguments)]
    async fn search_bands(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        k: usize,
        allowed: Option<&RoaringBitmap>,
        mode: SearchMode<'_>,
        bands: &experiment::Bands,
    ) -> io::Result<(Vec<SearchHit>, SearchMetrics)> {
        let prefetch = !matches!(mode, SearchMode::Demand);
        let threshold = if let SearchMode::Shared(threshold) = mode {
            Some(threshold)
        } else {
            None
        };
        params.validate().map_err(|e| input(&e))?;
        self.ensure_lengths().await?;
        let mut metrics = SearchMetrics::default();
        if k == 0 || query.tokens.is_empty() {
            return Ok((Vec::new(), metrics));
        }
        // A fully live, unfiltered segment needs no per-posting bitmap probe.
        // Check the actual range, rather than cardinality, because callers may
        // supply a bitmap containing ordinals outside this segment.
        let allowed = allowed.filter(|rows| !rows.contains_range(0..self.docs));
        let exact = query.exact_terms();
        let mut selections: BTreeMap<u64, (u32, bool, u64)> = BTreeMap::new();
        let exact_terms: Vec<&str> = exact.keys().copied().collect();
        let found = self.lookup(&exact_terms).await?;
        for ((term, qf), id) in exact.iter().zip(found) {
            if let Some(id) = id {
                selections.insert(
                    id,
                    (
                        *qf,
                        false,
                        *stats.document_frequencies.get(*term).unwrap_or(&0),
                    ),
                );
            }
        }
        if let Some(prefix) = query.prefix() {
            for id in self.prefix_terms(prefix).await? {
                selections.entry(id).or_insert((0, false, 0)).1 = true;
            }
        }
        let saturation = self.saturation(stats, params);
        let lengths = self.lens();
        let metas = self.load(selections.keys().copied()).await?;
        let windows = (self.num_docs() as usize).div_ceil(BLOCK);
        // Bound auxiliary window metadata, including broad prefix expansions.
        let cached_windows = if windows.saturating_mul(selections.len()) <= 65_536 {
            windows
        } else {
            0
        };
        let mut terms: Vec<_> = selections
            .iter()
            .map(|(&id, &(qf, prefix, df))| {
                SearchTerm::new(
                    metas[&id].clone(),
                    TermWeight::new(qf, df, stats, params),
                    prefix,
                )
            })
            .collect();
        if terms.is_empty() {
            return Ok((Vec::new(), metrics));
        }
        {
            let mut memo = self.decoded.lock().unwrap();
            for term in &mut terms {
                // Dense frames are consulted only after window pruning. Probing
                // them here hashes and retains every common-word frame, including
                // those that cannot affect the top k.
                term.postings = if self.sparse_term(term.meta.df) {
                    term.meta
                        .blocks
                        .iter()
                        .map(|b| memo.get(b.offset))
                        .collect()
                } else {
                    vec![None; term.meta.blocks.len()]
                };
                term.small = prefetch
                    && !term.prefix
                    && (term.meta.blocks.len() <= SMALL_BLOCKS
                        || (self.sparse_term(term.meta.df)
                            && term.postings.iter().all(Option::is_some)));
            }
        }
        // Sparse partials and final scores each accumulate at most n positive
        // terms. Two n-unit-roundoff allowances fit within n * EPSILON; the
        // additional margin covers bounds and the prefix contribution.
        let roundoff = (terms.len() as f64 + 8.0) * f64::from(f32::EPSILON);
        let margin = if roundoff < 1.0 {
            1.0 / (1.0 - roundoff)
        } else {
            f64::INFINITY
        };
        let mut bounds = vec![0.0f64; windows];
        let mut prefix_windows = vec![false; windows];
        let mut heap: BinaryHeap<Reverse<SearchHit>> = BinaryHeap::new();
        if prefetch {
            let wanted = terms
                .iter()
                .enumerate()
                .filter(|(_, term)| term.small)
                .flat_map(|(t, term)| {
                    term.postings
                        .iter()
                        .enumerate()
                        .filter(|(_, p)| p.is_none())
                        .map(move |(b, _)| (t, b))
                })
                .collect();
            self.read_search_blocks(&mut terms, wanted, &saturation, &mut metrics)
                .await?;
        }
        if terms.iter().all(|term| term.small) {
            // Every posting is already available. Accumulate once in reference
            // order instead of building window bounds and scoring those rows again.
            let mut scores = vec![0.0f32; self.docs as usize];
            let mut touched = Vec::new();
            for term in &terms {
                for postings in term.postings.iter().flatten() {
                    for &(row, tf) in postings.iter() {
                        if allowed.is_some_and(|rows| !rows.contains(row)) {
                            continue;
                        }
                        let score =
                            term.weight
                                .score_cached(tf, lengths[row as usize], &saturation);
                        if score <= 0.0 {
                            continue;
                        }
                        if scores[row as usize] == 0.0 {
                            touched.push(row);
                        }
                        scores[row as usize] += score;
                    }
                }
            }
            metrics.rows_scored = touched.len();
            let floor = search_floor(&heap, k, threshold);
            for row in touched {
                if floor.is_some_and(|floor| f64::from(scores[row as usize]) < floor) {
                    continue;
                }
                let hit = SearchHit {
                    row,
                    score: scores[row as usize],
                };
                if heap.len() < k {
                    heap.push(Reverse(hit));
                } else if hit > heap.peek().unwrap().0 {
                    *heap.peek_mut().unwrap() = Reverse(hit);
                }
            }
            if let Some(threshold) = threshold {
                threshold.publish(&heap);
            }
            let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
            hits.sort_unstable_by(|a, b| b.cmp(a));
            return Ok((hits, metrics));
        }
        let mut row_bounds = if terms.iter().any(|term| term.small) {
            vec![0.0f32; self.docs as usize]
        } else {
            Vec::new()
        };
        for term in &mut terms {
            term.cache_windows(cached_windows);
        }
        {
            let mut maxima = saturation.maxima.lock().unwrap();
            for term in &mut terms {
                let mut best = vec![0.0f64; windows];
                if term.small {
                    for postings in term.postings.iter().flatten() {
                        for &(row, tf) in postings.iter() {
                            let score =
                                term.weight
                                    .score_cached(tf, lengths[row as usize], &saturation);
                            row_bounds[row as usize] += score;
                            best[row as usize / BLOCK] =
                                best[row as usize / BLOCK].max(f64::from(score));
                        }
                    }
                } else {
                    let key = term.meta.blocks.first().map(|b| b.offset);
                    let mut apply = |normalized: &[f64]| {
                        for (w, &maximum) in normalized.iter().enumerate() {
                            if maximum >= 0.0 {
                                best[w] = f64::from(
                                    term.weight.score_saturation(maximum)
                                        * (1.0 + 8.0 * f32::EPSILON),
                                );
                                prefix_windows[w] |= term.prefix;
                            }
                        }
                    };
                    if let Some(cached) = key.and_then(|key| maxima.term(key)) {
                        // Reuse query-independent window maxima while the set of
                        // decoded frame bounds is unchanged. New frames invalidate
                        // this summary, allowing their tighter bounds to take effect.
                        apply(cached);
                    } else {
                        let mut normalized = vec![-1.0f64; windows];
                        for b in &term.meta.blocks {
                            if let Some(windows) = maxima.frames.get(&b.offset) {
                                for &(w, maximum) in windows {
                                    normalized[w] = normalized[w].max(maximum);
                                }
                            } else {
                                let maximum = saturation.factor(b.max_tf, b.min_length).max(0.0);
                                for value in &mut normalized
                                    [b.first as usize / BLOCK..=b.last as usize / BLOCK]
                                {
                                    *value = value.max(maximum);
                                }
                            }
                        }
                        apply(&normalized);
                        if let Some(key) = key {
                            maxima.remember_term(key, normalized);
                        }
                    }
                    for (bound, &value) in bounds.iter_mut().zip(&best) {
                        *bound += value;
                    }
                }
                if cached_windows > 0 {
                    term.bounds = best;
                }
            }
        }
        let dense_bounds = bounds.clone();
        let band_terms: Vec<_> = selections
            .keys()
            .enumerate()
            .filter_map(|(t, handle)| bands.terms.get(handle).map(|b| (t, b)))
            .collect();
        let mut band_masks = vec![[u64::MAX; 2]; windows];
        let mut band_rows = 0usize;
        let mut band_windows = 0usize;
        // Known sparse terms must co-occur at a row, rather than at unrelated
        // maxima in the same window. This tightens the bound before any dense read.
        for (w, scores) in row_bounds.chunks(BLOCK).enumerate() {
            bounds[w] += f64::from(scores.iter().copied().fold(0.0, f32::max));
        }
        let mut order: Vec<_> = bounds
            .into_iter()
            .enumerate()
            .map(|(w, b)| (w, (b + f64::from(prefix_windows[w])) * margin))
            .filter(|(_, b)| *b > 0.0)
            .collect();
        order.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        let mut next = 0;
        let mut batch_size = if prefetch { FIRST_WINDOWS } else { usize::MAX };
        let mut by_bound = Vec::with_capacity(terms.len());
        let mut essential = vec![true; terms.len()];
        while next < order.len() {
            let mut batch = Vec::new();
            while next < order.len() && batch.len() < batch_size {
                let (window, bound) = order[next];
                if search_floor(&heap, k, threshold).is_some_and(|floor| bound < floor) {
                    // Bounds descend, and the heap threshold only rises.
                    metrics.windows_pruned += order.len() - next;
                    next = order.len();
                    break;
                }
                next += 1;
                let start = (window * BLOCK) as u32;
                let end = ((window + 1) * BLOCK).min(self.num_docs() as usize) as u32;
                if allowed.is_some_and(|rows| rows.range(start..end).next().is_none()) {
                    continue;
                }
                if let Some(floor) = search_floor(&heap, k, threshold)
                    && !band_terms.is_empty()
                {
                    let mut other = 0.0f64;
                    for (t, term) in terms.iter().enumerate() {
                        if !term.small && !band_terms.iter().any(|(bt, _)| *bt == t) {
                            other += term.bound(window, &saturation);
                        }
                    }
                    let mut upper = [other; BLOCK];
                    for &(t, term_bands) in &band_terms {
                        let term = &terms[t];
                        for (b, bitmap) in term_bands.iter().enumerate() {
                            let score = f64::from(term.weight.score_saturation(bands.ceilings[b]));
                            for lane in 0..2 {
                                let mut mask = bitmap[window * 2 + lane];
                                while mask != 0 {
                                    let bit = mask.trailing_zeros() as usize;
                                    upper[lane * 64 + bit] += score;
                                    mask &= mask - 1;
                                }
                            }
                        }
                    }
                    let mut masks = [0u64; 2];
                    for (i, &upper) in upper.iter().enumerate().take((end - start) as usize) {
                        let sparse = row_bounds.get(start as usize + i).copied().unwrap_or(0.0);
                        if (upper + f64::from(sparse)) * margin >= floor {
                            masks[i / 64] |= 1 << (i % 64);
                        }
                    }
                    band_rows += (end - start) as usize
                        - masks.iter().map(|m| m.count_ones() as usize).sum::<usize>();
                    band_masks[window] = masks;
                    if masks == [0; 2] {
                        band_windows += 1;
                        continue;
                    }
                }
                batch.push((window, bound));
            }
            if batch.is_empty() {
                break;
            }
            if std::env::var_os("BF_TRACE_STAGES").is_some() {
                eprintln!(
                    "FTS batch windows={} of {} first_bound={:.2} last_bound={:.2} heap_min={:?}",
                    batch.len(),
                    order.len(),
                    batch[0].1,
                    batch.last().unwrap().1,
                    heap.peek().map(|h| h.0.score)
                );
            }
            batch_size = usize::MAX;
            if prefetch {
                let mut wanted = Vec::new();
                // A frame can intersect several windows. Sweep their union in
                // row order, requesting each frame once, without sorting a much
                // larger vector of duplicate term/frame pairs. Scoring retains
                // the original descending-bound window order.
                let mut windows: Vec<_> = batch.iter().map(|&(w, _)| w).collect();
                windows.sort_unstable();
                for (t, term) in terms.iter().enumerate() {
                    let mut after = 0;
                    for &window in &windows {
                        let span = term.span(window);
                        for b in span.start.max(after)..span.end {
                            let meta = &term.meta.blocks[b];
                            if term.postings[b].is_none()
                                && allowed.is_none_or(|rows| {
                                    rows.range(meta.first..=meta.last).next().is_some()
                                })
                            {
                                wanted.push((t, b));
                            }
                        }
                        after = after.max(span.end);
                    }
                }
                self.read_search_blocks(&mut terms, wanted, &saturation, &mut metrics)
                    .await?;
            }
            for (window, bound) in batch {
                if search_floor(&heap, k, threshold).is_some_and(|floor| bound < floor) {
                    metrics.windows_pruned += 1;
                    continue;
                }
                let start = (window * BLOCK) as u32;
                let end = ((window + 1) * BLOCK).min(self.num_docs() as usize) as u32;
                if !prefetch {
                    let mut wanted = Vec::new();
                    for (t, term) in terms.iter().enumerate() {
                        for b in term.span(window) {
                            let meta = &term.meta.blocks[b];
                            if term.postings[b].is_none()
                                && allowed.is_none_or(|rows| {
                                    rows.range(meta.first..=meta.last).next().is_some()
                                })
                            {
                                wanted.push((t, b));
                            }
                        }
                    }
                    self.read_search_blocks(&mut terms, wanted, &saturation, &mut metrics)
                        .await?;
                }
                let floor = search_floor(&heap, k, threshold);
                let mut candidates = [true; BLOCK];
                if let Some(floor) = floor {
                    // Block-max MaxScore: low-impact terms cannot create a hit on
                    // their own. Generate candidates only from the essential terms.
                    // Strict inequality retains every score tie. Prefix expansions
                    // each get a conservative flat contribution, counted only once
                    // when computing the final score.
                    by_bound.clear();
                    by_bound.extend(
                        terms
                            .iter()
                            .enumerate()
                            .map(|(t, term)| (t, term.bound(window, &saturation))),
                    );
                    by_bound.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
                    essential.fill(true);
                    let mut nonessential = 0.0;
                    let mut skipped = 0;
                    for &(t, bound) in &by_bound {
                        if (nonessential + bound) * margin >= floor {
                            break;
                        }
                        nonessential += bound;
                        essential[t] = false;
                        skipped += 1;
                    }
                    if skipped > 0 {
                        candidates.fill(false);
                        for (_, term) in terms.iter().enumerate().filter(|(t, _)| essential[*t]) {
                            for b in term.span(window) {
                                let Some(postings) = &term.postings[b] else {
                                    continue;
                                };
                                let lo = if term.meta.blocks[b].first < start {
                                    postings.partition_point(|&(row, _)| row < start)
                                } else {
                                    0
                                };
                                for &(row, _) in
                                    postings[lo..].iter().take_while(|&&(row, _)| row < end)
                                {
                                    candidates[(row - start) as usize] = true;
                                }
                            }
                        }
                    }
                }
                // Test the sparse bound and allowed mask once per candidate, not
                // again for every term that hits it.
                let competitive: Vec<_> = (start..end)
                    .filter(|&row| {
                        candidates[(row - start) as usize]
                            && band_masks[window][(row as usize % BLOCK) / 64]
                                & (1 << (row as usize % 64))
                                != 0
                            && allowed.is_none_or(|rows| rows.contains(row))
                            && floor.is_none_or(|floor| {
                                row_bounds.is_empty()
                                    || (f64::from(row_bounds[row as usize])
                                        + dense_bounds[window]
                                        + f64::from(prefix_windows[window]))
                                        * margin
                                        >= floor
                            })
                    })
                    .collect();
                if competitive.is_empty() {
                    continue;
                }
                candidates.fill(false);
                for &row in &competitive {
                    candidates[(row - start) as usize] = true;
                }
                let mut scores = [0.0f32; BLOCK];
                let mut prefix_matches = [false; BLOCK];
                for term in &terms {
                    for b in term.span(window) {
                        let Some(postings) = &term.postings[b] else {
                            continue;
                        };
                        // The directory already proves either endpoint when the
                        // frame begins or ends inside this window. Search only
                        // the remaining boundary, within the retained suffix.
                        let block = &term.meta.blocks[b];
                        let lo = if block.first < start {
                            postings.partition_point(|&(row, _)| row < start)
                        } else {
                            0
                        };
                        let hi = if block.last >= end {
                            lo + postings[lo..].partition_point(|&(row, _)| row < end)
                        } else {
                            postings.len()
                        };
                        let postings = &postings[lo..hi];
                        let mut add = |row: u32, tf| {
                            let i = (row - start) as usize;
                            scores[i] +=
                                term.weight
                                    .score_cached(tf, lengths[row as usize], &saturation);
                            prefix_matches[i] |= term.prefix;
                        };
                        if competitive.len() * 8 < postings.len() {
                            // A common-word frame can have 128 entries but only one
                            // rare-term candidate. Seek those rows instead of scanning
                            // the entire frame and testing the same mask repeatedly.
                            for &row in &competitive {
                                if let Ok(i) = postings.binary_search_by_key(&row, |&(r, _)| r) {
                                    add(row, postings[i].1);
                                }
                            }
                        } else {
                            for &(row, tf) in postings {
                                if candidates[(row - start) as usize] {
                                    add(row, tf);
                                }
                            }
                        }
                    }
                }
                for row in competitive {
                    let i = (row - start) as usize;
                    let score = scores[i] + f32::from(prefix_matches[i]);
                    if score <= 0.0 {
                        continue;
                    }
                    metrics.rows_scored += 1;
                    let hit = SearchHit { row, score };
                    if heap.len() < k {
                        heap.push(Reverse(hit));
                    } else if hit > heap.peek().unwrap().0 {
                        *heap.peek_mut().unwrap() = Reverse(hit);
                    }
                }
                if let Some(threshold) = threshold {
                    threshold.publish(&heap);
                }
            }
        }
        let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
        hits.sort_unstable_by(|a, b| b.cmp(a));
        bands
            .skipped_rows
            .fetch_add(band_rows, std::sync::atomic::Ordering::Relaxed);
        bands
            .skipped_windows
            .fetch_add(band_windows, std::sync::atomic::Ordering::Relaxed);
        Ok((hits, metrics))
    }
}
