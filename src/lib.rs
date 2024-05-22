use std::path::{
    MAIN_SEPARATOR,
    Path,
    PathBuf,
};

use anyhow::Result;
use glob::glob;
use polars::prelude::*;

pub fn find_dir_by_pattern(base_dir: &PathBuf, dir_pattern: &str) -> PathBuf {
    let pattern = format!("{}{}{}", base_dir.to_string_lossy(), MAIN_SEPARATOR, dir_pattern);
    let dirs: Vec<_> = glob(&pattern).expect("Failed to read glob pattern")
        .filter_map(std::result::Result::ok)
        .filter(|path| path.is_dir())
        .collect();

    match dirs.len() {
        1 => dirs[0].clone(),
        0 => panic!("No directory matching '{}' found in {:?}", dir_pattern, base_dir),
        _ => panic!("Multiple directories matching '{}' found in {:?}", dir_pattern, base_dir),
    }
}

pub fn make_iiq_df(iiq_files: &[PathBuf]) -> PolarsResult<DataFrame> {
    // Filenames match pattern yyyy-mm-ddnnn_RGB_id.IIQ

    df!(
        "Path" => &iiq_files.iter().map(|p| p.to_string_lossy().into_owned()).collect::<Vec<String>>(),
        "Filename" => &iiq_files.iter().map(|p| p.file_name().unwrap().to_string_lossy().into_owned()).collect::<Vec<String>>(),
        "Date" => &iiq_files.iter().map(|p| p.file_name().unwrap().to_string_lossy()[..10].to_string()).collect::<Vec<String>>(),
        "Event" => &iiq_files.iter().map(|p| p.file_name().unwrap().to_string_lossy().split('_').next().unwrap().to_string()[10..].parse::<i32>().unwrap()).collect::<Vec<i32>>(),
        "Type" => &iiq_files.iter().map(|p| p.file_name().unwrap().to_string_lossy().split('_').nth(1).unwrap().to_string()).collect::<Vec<String>>(),
        "ID" => &iiq_files.iter().map(|p| {
            let id = p.file_name().unwrap().to_string_lossy().split('_').nth(2).unwrap().to_string();
            id.trim_end_matches(".IIQ").parse::<i32>().unwrap()
        }).collect::<Vec<i32>>(),
    )
}

pub fn find_files(dir: &Path, extension: &str) -> Vec<PathBuf> {
    dir.read_dir().unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().unwrap().is_file())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(extension))
        .map(|entry| entry.path())
        .collect()
}

pub fn move_unmatched_files(df: &DataFrame, dir: &Path, column_name: &str, subdir_name: &str, dry_run: bool) -> Result<()> {
    let path_series = df.column(column_name).unwrap().str().unwrap();
    let paths: Vec<PathBuf> = path_series.into_iter().filter_map(|s| s.map(PathBuf::from)).collect();

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