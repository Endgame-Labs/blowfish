//! Probe budgeting shared by the asynchronous and compiled tier planners.

/// Unrounded probe budget for one spatial tier, before live-row or delta-share adjustments.
///
/// The population divided by the number of leaves identifies the stored layout, so old tiers
/// retain their plan until rebuilt. Round that population to a 32-row scan block to avoid
/// changing the plan because a builder rounded its number of clusters. A 200-row layout keeps
/// the original square-root budget; 640-row leaves use 70/64 of it (10M). Other leaf
/// populations keep the original budget: NQ's intermediate-leaf experiment failed the warm
/// gate, and no intermediate layout is selected by the automatic builder.
/// The caller rounds up after applying any filter, mask, or cross-tier adjustment.
///
/// `nprobe` is a quality budget, rather than a literal number of leaves: the historical planner
/// already scales it by leaf count over the square root of the population.
pub fn tier_probe_budget(nprobe: usize, rows: usize, clusters: usize) -> f64 {
    if rows == 0 || clusters == 0 {
        return 0.0;
    }
    let leaf = ((rows as f64 / clusters as f64 / 32.0).round() * 32.0).clamp(200.0, 640.0);
    // 56 matched the historical 200-row layout. A fresh 200-row build has higher recall;
    // 70 matches that control, so an ordinary recluster does not trade away its quality.
    let adjustment = if leaf >= 640.0 { 70.0 / 64.0 } else { 1.0 };
    nprobe as f64 * adjustment * (clusters as f64 / (rows as f64).sqrt()).max(1.0)
}

#[cfg(test)]
mod tests {
    use super::tier_probe_budget;

    #[test]
    fn stored_population_selects_the_recall_budget() {
        let old = tier_probe_budget(64, 10_000_000, 50_000);
        assert_eq!(old, 64.0 * 50_000.0 / 10_000_000_f64.sqrt());
        // Small variations in cluster count do not change a layout's quality budget.
        let original = 64.0 * 49_900.0 / 10_000_000_f64.sqrt();
        assert_eq!(tier_probe_budget(64, 10_000_000, 49_900), original);
        let coarse = tier_probe_budget(64, 10_000_000, 15_611);
        let measured = 70.0 * 15_611.0 / 10_000_000_f64.sqrt();
        assert!((coarse - measured).abs() < 1e-10);
        let intermediate = tier_probe_budget(64, 2_681_468, 8_360);
        let unadjusted = 64.0 * 8_360.0 / 2_681_468_f64.sqrt();
        assert!((intermediate - unadjusted).abs() < 1e-10);
    }

    #[test]
    fn empty_and_coarse_layouts_have_bounded_budgets() {
        assert_eq!(tier_probe_budget(64, 0, 0), 0.0);
        assert_eq!(tier_probe_budget(64, 10, 0), 0.0);
        assert_eq!(tier_probe_budget(0, 100, 1), 0.0);
        assert_eq!(tier_probe_budget(64, 100_000_000, 1), 70.0);
    }
}
