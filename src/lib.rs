use std::path::{Path, PathBuf, MAIN_SEPARATOR};

use anyhow::Result;
use glob::glob;
use polars::prelude::*;

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
    // Filenames match pattern yyyy-mm-ddnnn_RGB_id.IIQ
    let filenames = &iiq_files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect::<Vec<String>>();

    df!(
        "Path" => &iiq_files
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect::<Vec<String>>(),
        "Filename" => filenames,
        "Date" => &filenames
            .iter()
            .map(|p| p[..10].to_string())
            .collect::<Vec<String>>(),
        "Event" => &filenames
            .iter()
            .map(|p| p.split('_').next().unwrap()[10..].to_string())
            .collect::<Vec<String>>(),
        "Type" => &filenames
            .iter()
            .map(|p| p.split('_').nth(1).unwrap().to_string())
            .collect::<Vec<String>>(),
        "ID" => &filenames
            .iter()
            .map(|p| {
                let id = p.split('_').nth(2).unwrap().to_string();
                id.trim_end_matches(".IIQ").parse::<i32>().unwrap()
            })
            .collect::<Vec<i32>>(),
    )
}

pub fn find_files(dir: &Path, extension: &str) -> Result<Vec<PathBuf>> {
    let files = dir
        .read_dir()?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().unwrap().is_file())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(extension))
        .map(|entry| entry.path())
        .collect();
    Ok(files)
}

pub fn move_unmatched_files(
    df: &DataFrame,
    dir: &Path,
    column_name: &str,
    subdir_name: &str,
    dry_run: bool,
) -> Result<()> {
    let path_series = df.column(column_name)?.str().unwrap();
    let paths: Vec<PathBuf> = path_series
        .into_iter()
        .filter_map(|s| s.map(PathBuf::from))
        .collect();

    // Create 'unmatched' directory
    let unmatched_dir = dir.join(subdir_name);
    if !dry_run {
        std::fs::create_dir_all(&unmatched_dir)?;
    }

    // Move files to 'unmatched' directory
    for path in paths {
        let dest = unmatched_dir.join(path.file_name().unwrap());
        println!("{} -> {}", path.display(), dest.display());
        if !dry_run {
            std::fs::rename(&path, &dest)?;
        }
    }

    Ok(())
}
