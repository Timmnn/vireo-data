use super::Generator;
use chrono::prelude::*;
use polars::prelude::*;
use rand::thread_rng;
use rand_distr::{Distribution, Normal};

pub struct EquitiesGenerator {}

impl Generator for EquitiesGenerator {
    fn generate(config: &crate::config_reader::DatasetConfig) -> DataFrame {
        // More realistic initial parameters
        let starting_price = 100.0; // Initial stock price
        let volatility = 0.02; // 2% daily volatility
        let mut time = config.daterange.from;

        let mut rng = thread_rng();

        // Normal distribution for price changes with lower standard deviation
        let price_change_dist = Normal::new(0.0, volatility).unwrap();

        // Create a mutable DataFrame using vec![]
        let mut columns = vec![
            Series::new("open".into(), Vec::<f64>::new()).into(),
            Series::new("high".into(), Vec::<f64>::new()).into(),
            Series::new("low".into(), Vec::<f64>::new()).into(),
            Series::new("close".into(), Vec::<f64>::new()).into(),
            Series::new("volume".into(), Vec::<u32>::new()).into(),
            Series::new("datetime".into(), Vec::<NaiveDateTime>::new()).into(),
        ];

        let mut df = DataFrame::new(columns).unwrap();

        let mut prev_close = starting_price;

        while time <= config.daterange.to {
            // More controlled price generation
            let price_change_pct: f64 = price_change_dist.sample(&mut rng);

            // Calculate open price with a smoother change
            let open = prev_close * (1.0 + price_change_pct);

            // Generate high and low with more realistic bounds
            let high_change = (1.0 + price_change_dist.sample(&mut rng) * 2.0).abs() as f64;
            let low_change = (1.0 - price_change_dist.sample(&mut rng) * 2.0).abs() as f64;

            let high = open * high_change;
            let low = open * low_change.min(1.0);

            // Close price as a point between low and high
            let close = low + (high - low) * 0.5;

            // More realistic volume generation
            let base_volume = 50_000;
            let volume_variation = (price_change_pct.abs() * 100_000.0).abs() as u32;
            let volume = base_volume + volume_variation;

            // Create datetime with time set to midnight

            // Create a new row
            let new_row = df!(
                "open" => vec![open],
                "high" => vec![high],
                "low" => vec![low],
                "close" => vec![close],
                "volume" => vec![volume],
                "datetime" => vec![time]
            )
            .unwrap();

            // Vstack the new row
            df = df.vstack(&new_row).unwrap();

            // Update for next iteration
            prev_close = close;
            time = time.checked_add_days(chrono::Days::new(1)).unwrap();
        }

        return df;
    }
}
