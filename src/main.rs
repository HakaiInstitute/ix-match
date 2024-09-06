use anyhow::Result;
use clap::Parser;
use polars::prelude::*;

use std::path::PathBuf;
use std::time::Duration;

use ix_match::{find_dir_by_pattern, find_files, make_iiq_df, move_files};

/// Match RGB and NIR IIQ files and move unmatched images to a new subdirectory.
/// Helps to sort images from an aerial survey using PhaseOne cameras as a preprocessing step for
/// converting the files with IX-Capture.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Directory containing the RGB and NIR subdirectories, which contain the IIQ files
    #[arg(default_value = ".")]
    iiq_dir: PathBuf,

    /// Dry run (do not move files)
    #[arg(short, long, action = clap::ArgAction::SetTrue, default_value = "false")]
    dry_run: bool,

    /// Pattern for finding directory containing RGB files
    #[arg(short, long, default_value = "C*_RGB")]
    rgb_pattern: String,

    /// Pattern for finding directory containing NIR files
    #[arg(short, long, default_value = "C*_NIR")]
    nir_pattern: String,

    /// Threshold for matching images in milliseconds
    #[arg(short, long, default_value = "500")]
    thresh: u64,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let iiq_dir = args.iiq_dir;

    let rgb_dir = find_dir_by_pattern(&iiq_dir, &args.rgb_pattern)
        .ok_or_else(|| anyhow::anyhow!("RGB directory not found"))?;

    let nir_dir = find_dir_by_pattern(&iiq_dir, &args.nir_pattern)
        .ok_or_else(|| anyhow::anyhow!("NIR directory not found"))?;

    // Find IIQ files
    let rgb_iiq_files = find_files(&rgb_dir, ".iiq")?;
    let nir_iiq_files = find_files(&nir_dir, ".iiq")?;

    // Create dataframes
    let rgb_df = make_iiq_df(&rgb_iiq_files)?;
    let nir_df = make_iiq_df(&nir_iiq_files)?;

    // Do the join
    let joint_df = ix_match::join_dataframes(&rgb_df, &nir_df)?;

    // Split df into matched and unmatched based on threshold
    let thresh = Duration::from_millis(args.thresh).as_nanos() as i64;
    let thresh_exp = lit(thresh).cast(DataType::Duration(TimeUnit::Nanoseconds));

    let matched_df = joint_df
        .clone()
        .lazy()
        .filter(col("dt").lt_eq(thresh_exp.clone()))
        .collect()?;

    let unmatched_rgb_df = joint_df
        .clone()
        .lazy()
        .filter(col("dt").gt(thresh_exp.clone()))
        .select(&[col("Path_rgb")])
        .drop_nulls(None)
        .collect()?;

    let unmatched_nir_df = joint_df
        .clone()
        .lazy()
        .filter(col("dt").gt(thresh_exp.clone()))
        .select(&[col("Path_nir")])
        .drop_nulls(None)
        .collect()?;

    if !args.dry_run {
        // Create the unmatched directories
        let unmatched_rgb_dir = rgb_dir.join("unmatched");
        std::fs::create_dir_all(&unmatched_rgb_dir)?;

        let unmatched_nir_dir = nir_dir.join("unmatched");
        std::fs::create_dir_all(&unmatched_nir_dir)?;

        // Move all matched iiq files to camera dirs root
        move_files(&matched_df, &rgb_dir, "Path_rgb", args.verbose)?;
        move_files(&matched_df, &nir_dir, "Path_nir", args.verbose)?;

        // Move unmatched files
        if unmatched_rgb_df.height() > 0 {
            std::fs::create_dir_all(&unmatched_rgb_dir)?;
            move_files(
                &unmatched_rgb_df,
                &unmatched_rgb_dir,
                "Path_rgb",
                args.verbose,
            )?;
        }
        if unmatched_nir_df.height() > 0 {
            std::fs::create_dir_all(&unmatched_nir_dir)?;
            move_files(
                &unmatched_nir_df,
                &unmatched_nir_dir,
                "Path_nir",
                args.verbose,
            )?;
        }
    }

    println!(
        "RGB: {}, NIR: {} ({} match)",
        rgb_df.height(),
        nir_df.height(),
        matched_df.height()
    );

    Ok(())
}

// TODO
// - [ ] Add a GUI or TUI
