use anyhow::Result;
use chrono::prelude::*;
use glob::glob;
use polars::df;
use polars::prelude::*;

use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};

pub fn find_dir_by_pattern(base_dir: &PathBuf, dir_pattern: &str) -> Option<PathBuf> {
    let pattern = format!(
        "{}{}{}",
        base_dir.to_string_lossy(),
        MAIN_SEPARATOR,
        dir_pattern
    );
    let dirs: Vec<_> = glob(&pattern)
        .expect("Failed to read glob pattern")
        .filter_map(std::result::Result::ok)
        .filter(|path| path.is_dir())
        .collect();

    match dirs.len() {
        1 => Some(dirs[0].clone()),
        0 => {
            println!(
                "No directory matching '{}' found in {:?}",
                dir_pattern, base_dir
            );
            None
        }
        _ => {
            println!(
                "Multiple directories matching '{}' found in {:?}",
                dir_pattern, base_dir
            );
            None
        }
    }
}

pub fn make_iiq_df(iiq_files: &[PathBuf]) -> PolarsResult<DataFrame> {
    let paths: Vec<String> = iiq_files
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();

    let stems: Vec<String> = iiq_files
        .iter()
        .map(|p| p.file_stem().unwrap().to_string_lossy().into_owned())
        .collect();

    let datetimes: Vec<NaiveDateTime> = stems
        .iter()
        .map(|stem| NaiveDateTime::parse_from_str(&stem[..16], "%y%m%d_%H%M%S%3f").unwrap())
        .collect();

    df!(
        "Path" => paths,
        "Stem" => stems,
        "Datetime" => datetimes
    )
}

pub fn find_files(dir: &Path, extension: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    find_files_recursive(dir, extension, &mut files)?;
    Ok(files)
}

fn find_files_recursive(dir: &Path, extension: &str, files: &mut Vec<PathBuf>) -> Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                find_files_recursive(&path, extension, files)?;
            } else if path.is_file()
                && path.extension().and_then(|s| s.to_str())
                    == Some(extension.trim_start_matches('.'))
            {
                files.push(path);
            }
        }
    }
    Ok(())
}

pub fn move_files(df: &DataFrame, dir: &Path, column_name: &str, verbose: bool) -> Result<()> {
    let path_series = df.column(column_name)?.str().unwrap();
    let paths: Vec<PathBuf> = path_series
        .into_iter()
        .filter_map(|s| s.map(PathBuf::from))
        .collect();

    // Move files to 'unmatched' directory
    for path in paths {
        let dest = dir.join(path.file_name().unwrap());
        if verbose {
            println!("{} -> {}", path.display(), dest.display());
        }
        std::fs::rename(&path, &dest)?;
    }

    Ok(())
}

pub fn join_dataframes(rgb_df: &DataFrame, nir_df: &DataFrame) -> Result<DataFrame> {
    // Sort by datetime
    let mut rgb_df = rgb_df.sort(["Datetime"], SortMultipleOptions::default())?;
    let mut nir_df = nir_df.sort(["Datetime"], SortMultipleOptions::default())?;

    // Add a dummy column to both dataframes
    let dummy_series = Series::new("dummy", vec![1; rgb_df.height()]);
    rgb_df.with_column(dummy_series.clone())?;

    let dummy_series = Series::new("dummy", vec![1; nir_df.height()]);
    nir_df.with_column(dummy_series)?;

    // Rename the columns to avoid conflicts
    let mut rgb_df = rgb_df.clone();
    rgb_df.rename("Datetime", "Datetime_rgb")?;
    rgb_df.rename("Path", "Path_rgb")?;
    rgb_df.rename("Stem", "Stem_rgb")?;

    let mut nir_df = nir_df.clone();
    nir_df.rename("Datetime", "Datetime_nir")?;
    nir_df.rename("Path", "Path_nir")?;
    nir_df.rename("Stem", "Stem_nir")?;

    let matched_df = rgb_df.join_asof_by(
        &nir_df,
        "Datetime_rgb",
        "Datetime_nir",
        ["dummy"],
        ["dummy"],
        AsofStrategy::Nearest,
        None,
    )?;

    // Drop the dummy columns
    let mut matched_df = matched_df.drop("dummy")?;

    // Add a new column with the time difference
    let datetime_left = matched_df
        .column("Datetime_rgb")?
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
    let datetime_right = matched_df
        .column("Datetime_nir")?
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

    let time_diff = (datetime_left - datetime_right)?
        .rename("dt")
        .deref()
        .to_owned();

    matched_df.with_column(time_diff)?;

    Ok(matched_df)
}
