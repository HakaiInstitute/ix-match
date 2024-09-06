use anyhow::Result;
use chrono::prelude::*;
use glob::glob;
use polars::df;
use polars::prelude::*;

use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
use std::time::Duration;

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

fn make_iiq_df(iiq_files: &[PathBuf]) -> PolarsResult<DataFrame> {
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

fn find_files(dir: &Path, extension: &str) -> Result<Vec<PathBuf>> {
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

fn move_files(df: &DataFrame, dir: &Path, column_name: &str, verbose: bool) -> Result<()> {
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

fn join_dataframes(rgb_df: &DataFrame, nir_df: &DataFrame) -> Result<DataFrame> {
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

    let matched_df_rgb = rgb_df
        .join_asof_by(
            &nir_df,
            "Datetime_rgb",
            "Datetime_nir",
            ["dummy"],
            ["dummy"],
            AsofStrategy::Nearest,
            None,
        )?
        .lazy()
        .select(&[
            col("Path_rgb"),
            col("Stem_rgb"),
            col("Datetime_rgb"),
            col("Path_nir"),
            col("Stem_nir"),
            col("Datetime_nir"),
        ])
        .collect()?;

    let matched_df_nir = nir_df
        .join_asof_by(
            &rgb_df,
            "Datetime_nir",
            "Datetime_rgb",
            ["dummy"],
            ["dummy"],
            AsofStrategy::Nearest,
            None,
        )?
        .lazy()
        .select(&[
            col("Path_rgb"),
            col("Stem_rgb"),
            col("Datetime_rgb"),
            col("Path_nir"),
            col("Stem_nir"),
            col("Datetime_nir"),
        ])
        .collect()?;

    // Merge the two matched dataframes to imitate an outer join
    let matched_df =
        matched_df_rgb
            .vstack(&matched_df_nir)?
            .unique(None, UniqueKeepStrategy::Any, None)?;

    let mut matched_df = matched_df;

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
    let abs_time_diff = abs(&time_diff)?;

    matched_df.with_column(abs_time_diff)?;

    Ok(matched_df)
}

pub fn process_images(
    rgb_dir: &Path,
    nir_dir: &Path,
    threshold: Duration,
    dry_run: bool,
    verbose: bool,
) -> Result<(usize, usize, usize)> {
    // Find IIQ files
    let rgb_iiq_files = find_files(rgb_dir, ".iiq")?;
    let nir_iiq_files = find_files(nir_dir, ".iiq")?;

    // Create dataframes
    let rgb_df = make_iiq_df(&rgb_iiq_files)?;
    let nir_df = make_iiq_df(&nir_iiq_files)?;

    // Do the join
    let joint_df = join_dataframes(&rgb_df, &nir_df)?;

    // Split df into matched and unmatched based on threshold
    let thresh = threshold.as_nanos() as i64;
    let thresh_exp = lit(thresh).cast(DataType::Duration(TimeUnit::Nanoseconds));

    let matched_df = joint_df
        .clone()
        .lazy()
        .filter(col("dt").lt_eq(thresh_exp.clone()))
        .collect()?;

    let unmatched_rgb_df = joint_df
        .clone()
        .lazy()
        .join(
            matched_df.clone().lazy(),
            [col("Path_rgb")],
            [col("Path_rgb")],
            JoinArgs::new(JoinType::Anti),
        )
        .select(&[col("Stem_rgb"), col("Path_rgb")])
        .unique(None, UniqueKeepStrategy::Any)
        .collect()?;

    let unmatched_nir_df = joint_df
        .clone()
        .lazy()
        .join(
            matched_df.clone().lazy(),
            [col("Path_nir")],
            [col("Path_nir")],
            JoinArgs::new(JoinType::Anti),
        )
        .select([col("Stem_nir"), col("Path_nir")])
        .unique(None, UniqueKeepStrategy::Any)
        .collect()?;

    if verbose {
        println!("joint_df: {:?}", joint_df);
        println!("matched_df: {:?}", matched_df);
        println!("unmatched_rgb_df: {:?}", unmatched_rgb_df);
        println!("unmatched_nir_df: {:?}", unmatched_nir_df);
    }

    if !dry_run {
        // Move all matched iiq files to camera dirs root
        move_files(&matched_df, rgb_dir, "Path_rgb", verbose)?;
        move_files(&matched_df, nir_dir, "Path_nir", verbose)?;

        // Move unmatched files
        if unmatched_rgb_df.height() > 0 {
            let unmatched_rgb_dir = rgb_dir.join("unmatched");
            if verbose {
                println!("Moving unmatched RGB files to {:?}", unmatched_rgb_dir);
            }
            fs::create_dir_all(&unmatched_rgb_dir)?;
            move_files(&unmatched_rgb_df, &unmatched_rgb_dir, "Path_rgb", verbose)?;
        }
        if unmatched_nir_df.height() > 0 {
            let unmatched_nir_dir = nir_dir.join("unmatched");
            if verbose {
                println!("Moving unmatched NIR files to {:?}", unmatched_nir_dir);
            }
            fs::create_dir_all(&unmatched_nir_dir)?;
            move_files(&unmatched_nir_df, &unmatched_nir_dir, "Path_nir", verbose)?;
        }
    }

    Ok((rgb_df.height(), nir_df.height(), matched_df.height()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;
    use tempfile::TempDir;

    use std::fs;
    use std::time::Duration;

    #[test]
    fn test_find_dir_by_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        fs::create_dir(base_path.join("test_dir_123")).unwrap();
        fs::create_dir(base_path.join("another_dir_456")).unwrap();

        let result = find_dir_by_pattern(&base_path, "test_dir_*");
        assert!(result.is_some());
        assert_eq!(result.unwrap().file_name().unwrap(), "test_dir_123");

        let no_match = find_dir_by_pattern(&base_path, "nonexistent_*");
        assert!(no_match.is_none());

        fs::create_dir(base_path.join("CAMERA_RGB")).unwrap();
        let result = find_dir_by_pattern(&base_path, "C*_RGB");
        assert!(result.is_some());
        assert_eq!(result.unwrap().file_name().unwrap(), "CAMERA_RGB");

        fs::create_dir(base_path.join("Camera_NIR")).unwrap();
        let result = find_dir_by_pattern(&base_path, "C*_NIR");
        assert!(result.is_some());
        assert_eq!(result.unwrap().file_name().unwrap(), "Camera_NIR");
    }

    #[test]
    fn test_make_iiq_df() {
        let files = vec![
            PathBuf::from("/path/to/210101_120000000.iiq"),
            PathBuf::from("/path/to/210101_120001000.iiq"),
        ];

        let df = make_iiq_df(&files).unwrap();

        assert_eq!(df.shape(), (2, 3));
        assert_eq!(df.column("Path").unwrap().len(), 2);
        assert_eq!(df.column("Stem").unwrap().len(), 2);
        assert_eq!(df.column("Datetime").unwrap().len(), 2);

        let stems: Vec<&str> = df
            .column("Stem")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect::<Vec<Option<&str>>>()
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(stems, vec!["210101_120000000", "210101_120001000"]);
    }

    #[test]
    fn test_find_files() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        fs::write(base_path.join("test1.txt"), "content").unwrap();
        fs::write(base_path.join("test2.txt"), "content").unwrap();
        fs::write(base_path.join("test3.doc"), "content").unwrap();

        let txt_files = find_files(base_path, "txt").unwrap();
        assert_eq!(txt_files.len(), 2);

        let doc_files = find_files(base_path, "doc").unwrap();
        assert_eq!(doc_files.len(), 1);
    }

    #[test]
    fn test_join_dataframes() {
        let rgb_data = df!(
            "Datetime" => &[
                NaiveDateTime::parse_from_str("2021-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
                NaiveDateTime::parse_from_str("2021-01-01 12:00:01", "%Y-%m-%d %H:%M:%S").unwrap(),
            ],
            "Path" => &["/path/to/rgb1.iiq", "/path/to/rgb2.iiq"],
            "Stem" => &["rgb1", "rgb2"]
        )
        .unwrap();

        let nir_data = df!(
            "Datetime" => &[
                NaiveDateTime::parse_from_str("2021-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
                NaiveDateTime::parse_from_str("2021-01-01 12:00:02", "%Y-%m-%d %H:%M:%S").unwrap(),
            ],
            "Path" => &["/path/to/nir1.iiq", "/path/to/nir2.iiq"],
            "Stem" => &["nir1", "nir2"]
        )
        .unwrap();

        let result = join_dataframes(&rgb_data, &nir_data).unwrap();

        assert_eq!(result.shape(), (2, 7));
        assert!(result.column("dt").unwrap().null_count() == 0);
    }

    #[test]
    fn test_move_files() {
        let temp_dir = TempDir::new().unwrap();
        let source_dir = temp_dir.path().join("source");
        let dest_dir = temp_dir.path().join("dest");
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&dest_dir).unwrap();

        // Create test files
        fs::write(source_dir.join("file1.txt"), "content").unwrap();
        fs::write(source_dir.join("file2.txt"), "content").unwrap();

        let df = df!(
            "Path" => &[
                source_dir.join("file1.txt").to_string_lossy().into_owned(),
                source_dir.join("file2.txt").to_string_lossy().into_owned(),
            ]
        )
        .unwrap();

        move_files(&df, &dest_dir, "Path", false).unwrap();

        assert!(!source_dir.join("file1.txt").exists());
        assert!(!source_dir.join("file2.txt").exists());
        assert!(dest_dir.join("file1.txt").exists());
        assert!(dest_dir.join("file2.txt").exists());
    }

    #[test]
    fn test_process_images() {
        let temp_dir = TempDir::new().unwrap();
        let rgb_dir = temp_dir.path().join("rgb");
        let nir_dir = temp_dir.path().join("nir");
        fs::create_dir_all(&rgb_dir).unwrap();
        fs::create_dir_all(&nir_dir).unwrap();

        // Create test files
        fs::write(rgb_dir.join("210101_120000000.iiq"), "content").unwrap();
        fs::write(nir_dir.join("210101_120000100.iiq"), "content").unwrap();
        fs::write(rgb_dir.join("210101_120001000.iiq"), "content").unwrap();
        fs::write(nir_dir.join("210101_120001100.iiq"), "content").unwrap();

        let threshold = Duration::from_millis(200);
        let (rgb_count, nir_count, matched_count) =
            process_images(&rgb_dir, &nir_dir, threshold, false, false).unwrap();

        assert_eq!(rgb_count, 2);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 2);

        // Check if files are in their original locations
        // (process_images doesn't move matched files in this case)
        assert!(rgb_dir.join("210101_120000000.iiq").exists());
        assert!(rgb_dir.join("210101_120001000.iiq").exists());
        assert!(nir_dir.join("210101_120000100.iiq").exists());
        assert!(nir_dir.join("210101_120001100.iiq").exists());

        // Unmatched directories should not be created in this case
        assert!(!rgb_dir.join("unmatched").exists());
        assert!(!nir_dir.join("unmatched").exists());
    }

    #[test]
    fn test_find_files_recursive() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();
        let sub_dir = base_path.join("subdir");
        fs::create_dir_all(&sub_dir).unwrap();

        fs::write(base_path.join("test1.txt"), "content").unwrap();
        fs::write(base_path.join("test2.doc"), "content").unwrap();
        fs::write(sub_dir.join("test3.txt"), "content").unwrap();

        let mut files = Vec::new();
        find_files_recursive(base_path, "txt", &mut files).unwrap();

        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|f| f.file_name().unwrap() == "test1.txt"));
        assert!(files.iter().any(|f| f.file_name().unwrap() == "test3.txt"));
    }

    #[test]
    fn test_process_images_dry_run() {
        let temp_dir = TempDir::new().unwrap();
        let rgb_dir = temp_dir.path().join("rgb");
        let nir_dir = temp_dir.path().join("nir");
        fs::create_dir_all(&rgb_dir).unwrap();
        fs::create_dir_all(&nir_dir).unwrap();

        // Create test files
        fs::write(rgb_dir.join("210101_120000000.iiq"), "content").unwrap();
        fs::write(rgb_dir.join("210101_120001000.iiq"), "content").unwrap();
        fs::write(nir_dir.join("210101_120000100.iiq"), "content").unwrap();
        fs::write(nir_dir.join("210101_120005000.iiq"), "content").unwrap(); // This one won't match

        let threshold = Duration::from_millis(200);
        let (rgb_count, nir_count, matched_count) =
            process_images(&rgb_dir, &nir_dir, threshold, true, false).unwrap();

        assert_eq!(rgb_count, 2);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 1);

        // Check if all files are in their original locations (dry run)
        assert!(rgb_dir.join("210101_120000000.iiq").exists());
        assert!(rgb_dir.join("210101_120001000.iiq").exists());
        assert!(nir_dir.join("210101_120000100.iiq").exists());
        assert!(nir_dir.join("210101_120005000.iiq").exists());
        assert!(!rgb_dir.join("unmatched").exists());
        assert!(!nir_dir.join("unmatched").exists());
    }

    #[test]
    fn test_process_images_with_unmatched() {
        let temp_dir = TempDir::new().unwrap();
        let rgb_dir = temp_dir.path().join("rgb");
        let nir_dir = temp_dir.path().join("nir");
        fs::create_dir_all(&rgb_dir).unwrap();
        fs::create_dir_all(&nir_dir).unwrap();

        // Create test files
        fs::write(rgb_dir.join("210101_120000000.iiq"), "content").unwrap();
        fs::write(nir_dir.join("210101_120000100.iiq"), "content").unwrap();
        // These won't match
        fs::write(rgb_dir.join("210101_120001000.iiq"), "content").unwrap();
        fs::write(nir_dir.join("210101_120005000.iiq"), "content").unwrap();

        let threshold = Duration::from_millis(200);
        let (rgb_count, nir_count, matched_count) =
            process_images(&rgb_dir, &nir_dir, threshold, false, false).unwrap();

        assert_eq!(rgb_count, 2);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 1);

        // Check if matched files are in their original locations
        assert!(rgb_dir.join("210101_120000000.iiq").exists());
        assert!(nir_dir.join("210101_120000100.iiq").exists());

        // Check if unmatched files are moved to the unmatched directory
        assert!(rgb_dir
            .join("unmatched")
            .join("210101_120001000.iiq")
            .exists());
        assert!(!rgb_dir.join("210101_120001000.iiq").exists());
        assert!(nir_dir
            .join("unmatched")
            .join("210101_120005000.iiq")
            .exists());
        assert!(!nir_dir.join("210101_120005000.iiq").exists());
    }

    #[test]
    fn test_process_images_with_uneven_numbers() {
        let temp_dir = TempDir::new().unwrap();
        let rgb_dir = temp_dir.path().join("rgb");
        let nir_dir = temp_dir.path().join("nir");
        fs::create_dir_all(&rgb_dir).unwrap();
        fs::create_dir_all(&nir_dir).unwrap();

        // Create test files
        fs::write(rgb_dir.join("210101_120000000.iiq"), "content").unwrap();
        fs::write(nir_dir.join("210101_120000100.iiq"), "content").unwrap();
        // These won't match
        fs::write(nir_dir.join("210101_120005000.iiq"), "content").unwrap();

        let threshold = Duration::from_millis(200);
        let (rgb_count, nir_count, matched_count) =
            process_images(&rgb_dir, &nir_dir, threshold, false, false).unwrap();

        assert_eq!(rgb_count, 1);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 1);

        // Check if matched files are in their original locations
        assert!(rgb_dir.join("210101_120000000.iiq").exists());
        assert!(nir_dir.join("210101_120000100.iiq").exists());

        // Check if unmatched files are moved to the unmatched directory
        assert!(!rgb_dir.join("unmatched").exists());
        assert!(nir_dir
            .join("unmatched")
            .join("210101_120005000.iiq")
            .exists());
        assert!(!nir_dir.join("210101_120005000.iiq").exists());
    }
}
