use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use polars::prelude::*;

use ix_match::{find_dir_by_pattern, find_files, make_iiq_df, move_unmatched_files};

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

    /// The new subdirectory name where unmatched files will be moved
    #[arg(short, default_value = "Unmatched")]
    output_dir: String,

    /// Pattern for finding directory containing RGB files
    #[arg(short, long, default_value = "YC*")]
    rgb_pattern: String,

    /// Pattern for finding directory containing NIR files
    #[arg(short, long, default_value = "YD*")]
    nir_pattern: String,
}


fn main() -> Result<()> {
    let args = Args::parse();
    let iiq_dir = args.iiq_dir;

    // Find subdirectories beginning with "YC"
    let yc_dir = find_dir_by_pattern(&iiq_dir, args.rgb_pattern.as_str());
    let yc_iiq_files = find_files(&yc_dir, ".IIQ")?;

    // Find subdirectories beginning with "YD"
    let yd_dir = find_dir_by_pattern(&iiq_dir, args.nir_pattern.as_str());
    let yd_iiq_files = find_files(&yd_dir, ".IIQ")?;

    // Create dataframes
    let rgb_df = make_iiq_df(&yc_iiq_files)?;
    let nir_df = make_iiq_df(&yd_iiq_files)?;

    let matched_df = rgb_df.inner_join(&nir_df, &["Event"], &["Event"])?;
    println!("Found IIQs!");
    println!("RGB: {}, NIR: {} ({} match)", yc_iiq_files.len(), yd_iiq_files.len(), matched_df.height());

    let joined_df = rgb_df.outer_join(&nir_df, &["Event"], &["Event"])?;

    if matched_df.height() < joined_df.height() {
        println!("Moving unmatched files to '{}/' sub-directories", args.output_dir);
    } else {
        println!("All files matched!");
    }

    let mask = joined_df.column("Type")?.is_null();
    let unmatched_nir_df = joined_df.filter(&mask)?;
    if unmatched_nir_df.height() > 0 {
        move_unmatched_files(&unmatched_nir_df, &yd_dir, "Path_right", &args.output_dir, args.dry_run)?;
    }

    let mask = joined_df.column("Type_right")?.is_null();
    let unmatched_rgb_df = joined_df.filter(&mask)?;
    if unmatched_rgb_df.height() > 0 {
        move_unmatched_files(&unmatched_rgb_df, &yc_dir, "Path", &args.output_dir, args.dry_run)?;
    }

    Ok(())
}
