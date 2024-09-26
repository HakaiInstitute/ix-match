#![cfg(feature = "cli")]

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;

use ix_match::{find_dir_by_pattern, process_images};

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

    /// Keep empty files (do not filter out files with 0 bytes)
    #[arg(short, long, action = clap::ArgAction::SetTrue, default_value = "false")]
    keep_empty: bool,

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

    let thresh = Duration::from_millis(args.thresh);
    match process_images(
        &rgb_dir,
        &nir_dir,
        thresh,
        args.keep_empty,
        args.dry_run,
        args.verbose,
    ) {
        Ok((rgb_count, nir_count, matched_count, empty_rgb_files, empty_nir_files)) => {
            println!("RGB: {rgb_count}, NIR: {nir_count} ({matched_count} match)");
            println!("Empty files: RGB {empty_rgb_files}, NIR: {empty_nir_files}");
        }
        Err(e) => eprintln!("Error: {}", e),
    }

    Ok(())
}
