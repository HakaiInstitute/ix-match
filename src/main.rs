#![cfg(feature = "cli")]

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;

use ix_match::{find_dir_by_pattern, process_images, revert_changes};

fn parse_duration_millis(arg: &str) -> Result<Duration> {
    let millis = arg.parse::<u64>()?;
    Ok(Duration::from_millis(millis))
}

fn parse_canonical_path(arg: &str) -> Result<PathBuf> {
    let path = std::fs::canonicalize(arg)?;
    Ok(path)
}

/// Match RGB and NIR IIQ files and move unmatched images to a new subdirectory.
/// Helps to sort images from an aerial survey using PhaseOne cameras as a preprocessing step for
/// converting the files with IX-Capture.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Directory containing the RGB and NIR subdirectories, which contain the IIQ files
    #[arg(default_value = ".", value_parser = parse_canonical_path)]
    iiq_dir: PathBuf,

    /// Dry run (do not move files)
    #[arg(short, long, action = clap::ArgAction::SetTrue, default_value = "false")]
    dry_run: bool,

    /// Revert the operation (move files back to the original directories)
    /// This is useful if you want to undo the operation
    #[arg(short, long, action = clap::ArgAction::SetTrue, default_value = "false")]
    revert: bool,

    /// Keep empty files (do not filter out files with 0 bytes)
    #[arg(long, action = clap::ArgAction::SetTrue, default_value = "false")]
    keep_empty: bool,

    /// Pattern for finding directory containing RGB files
    #[arg(long, default_value = "CAMERA_RGB")]
    rgb_pattern: String,

    /// Pattern for finding directory containing NIR files
    #[arg(long, default_value = "CAMERA_NIR")]
    nir_pattern: String,

    /// Threshold for matching images in milliseconds
    #[arg(short, long, default_value = "500", value_parser = parse_duration_millis)]
    thresh: Duration,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Case-sensitive pattern matching on directory names
    #[arg(short, long, action=clap::ArgAction::SetTrue, default_value = "false")]
    case_sensitive: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let iiq_dir = args.iiq_dir;

    let rgb_dir = find_dir_by_pattern(&iiq_dir, &args.rgb_pattern, args.case_sensitive)
        .ok_or_else(|| anyhow::anyhow!("RGB directory not found"))?;

    let nir_dir = find_dir_by_pattern(&iiq_dir, &args.nir_pattern, args.case_sensitive)
        .ok_or_else(|| anyhow::anyhow!("NIR directory not found"))?;

    if args.revert {
        match revert_changes(&rgb_dir, &nir_dir, args.dry_run, args.verbose) {
            Ok((rgb_count, nir_count)) => {
                println!(
                    "RGB: {rgb_count}, NIR: {nir_count} files reverted to original directories"
                );
            }
            Err(e) => eprintln!("Error: {}", e),
        }
        return Ok(());
    }

    match process_images(
        &rgb_dir,
        &nir_dir,
        args.thresh,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_find_dir_by_pattern() {
        let iiq_dir = tempdir().unwrap().path().to_path_buf();
        let rgb_dir = iiq_dir.join("CAMERA_RGB/240101_1200");
        let nir_dir = iiq_dir.join("CAMERA_NIR/240101_1200");
        std::fs::create_dir_all(&rgb_dir).unwrap();
        std::fs::create_dir_all(&nir_dir).unwrap();

        let rgb_files = vec!["240101_1200_0001.iiq", "240101_1200_0002.iiq"];
        let nir_files = vec!["240101_1200_0001.iiq", "240101_1200_0002.iiq"];
        for file in rgb_files.iter().chain(nir_files.iter()) {
            std::fs::write(rgb_dir.join(file), "content").unwrap();
        }

        let args = Args::try_parse_from(vec!["."]).unwrap();
        let iiq_dir = args.iiq_dir;

        let rgb_dir = find_dir_by_pattern(&iiq_dir, &args.rgb_pattern, args.case_sensitive)
            .ok_or_else(|| anyhow::anyhow!("RGB directory not found"))?;

        let nir_dir = find_dir_by_pattern(&iiq_dir, &args.nir_pattern, args.case_sensitive)
            .ok_or_else(|| anyhow::anyhow!("NIR directory not found"))?;

        let thresh = Duration::from_millis(args.thresh);
        let (rgb_count, nir_count, matched_count, empty_rgb_files, empty_nir_files) =
            process_images(
                &rgb_dir,
                &nir_dir,
                thresh,
                args.keep_empty,
                args.dry_run,
                args.verbose,
            )
            .unwrap();

        assert_eq!(rgb_count, 2);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 2);
        assert_eq!(empty_rgb_files, 0);
        assert_eq!(empty_nir_files, 0);
    }
}
