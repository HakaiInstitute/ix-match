use anyhow::{anyhow, Context, Result};
use chrono::prelude::*;
use std::collections::HashMap;

use chrono::TimeDelta;
use std::fs;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::time::Duration;

mod filesystem;
pub use filesystem::find_dir_by_pattern;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct IIQFile {
    path: PathBuf,
    name: String,
    stem: String,
    datetime: NaiveDateTime,
    bytes: u64,
}

impl IIQFile {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let name = path.file_name().context("Failed to get file name")?.to_str().context("Failed to convert file name to string")?;
        let stem = path.file_stem().context("Failed to get file stem")?.to_str().context("Failed to convert file stem to string")?;
        let datetime = NaiveDateTime::parse_from_str(&stem[..16], "%y%m%d_%H%M%S%3f").context("Failed to parse datetime from stem")?;
        let bytes = path.metadata().context("Failed to get file metadata")?.len();
        Ok(IIQFile {
            path: path.to_owned(),
            name: name.to_owned(),
            stem: stem.to_owned(),
            datetime,
            bytes,
        })
    }

    fn diff(&self, other: &NaiveDateTime) -> TimeDelta {
        self.datetime.signed_duration_since(*other)
    }

    fn abs_diff(&self, other: &NaiveDateTime) -> Duration {
        Duration::from_millis(self.diff(other).num_milliseconds().abs() as u64)
    }
}


#[derive(Debug, Clone)]
struct IIQCollection {
    files: Vec<IIQFile>,
}

impl IIQCollection {
    pub fn new(paths: &[PathBuf]) -> Result<Self> {
        let mut files = paths.iter()
            .map(|p| IIQFile::new(p)).collect::<Result<Vec<IIQFile>>>()
            .context("Could not parse all files")?;
        // Sort files by datetime
        files.sort_by_key(|f| f.datetime);
        Ok(IIQCollection { files })
    }

    fn paths(&self) -> Vec<PathBuf> {
        self.files.iter().map(|f| f.path.clone()).collect()
    }

    fn len(&self) -> usize {
        self.files.len()
    }

    fn empty_files_len(&self) -> usize {
        self.files.iter().filter(|f| f.bytes == 0).count()
    }

    fn pop_empty_files(&mut self) -> IIQCollection {
        let (empty_files, non_empty_files): (Vec<IIQFile>, Vec<IIQFile>) =
            self.files.drain(..).partition(|f| f.bytes == 0);

        self.files = non_empty_files;

        IIQCollection { files: empty_files }
    }

    fn get_closest_file_by_datetime(&self, target_datetime: &NaiveDateTime) -> Result<&IIQFile> {
        if self.files.is_empty() {
            return Err(anyhow!("No files in collection"));
        }

        // Do binary search for the closest file
        let mut low = 0;
        let mut high = self.files.len() - 1;

        // Initialize closest diff
        let mut closest_diff = i64::MAX;
        let mut closest_file = None;

        while low <= high {
            let mid = (low + high) / 2;
            // Find diff in millis
            let diff = self.files[mid].diff(target_datetime).num_milliseconds().abs();
            if diff == 0 {
                return Ok(&self.files[mid]);
            }

            if diff < closest_diff || (diff == closest_diff && self.files[mid].datetime < *target_datetime) {
                closest_diff = diff;
                closest_file = Some(&self.files[mid]);
            }

            if self.files[mid].datetime < *target_datetime {
                low = mid + 1;
            } else if mid > 0 {
                high = mid - 1;
            } else {
                break;
            }
        }

        if let Some(closest_file) = closest_file {
            Ok(closest_file)
        } else {
            Err(anyhow!("Failed to get closest file by datetime"))
        }
    }
}

impl From<Vec<IIQFile>> for IIQCollection {
    fn from(files: Vec<IIQFile>) -> Self {
        IIQCollection { files }
    }
}

#[derive(Debug)]
struct JoinedIIQCollection<'a> {
    joined: Vec<(Option<&'a IIQFile>, Option<&'a IIQFile>, Duration)>,
}

impl<'a> JoinedIIQCollection<'a> {
    pub fn new(rgb: &'a IIQCollection, nir: &'a IIQCollection) -> Result<Self> {
        let rgb_shorter = rgb.len() < nir.len();
        let key_collection = if rgb_shorter { rgb } else { nir };
        let other_collection = if rgb_shorter { nir } else { rgb };

        let mut join_hash = other_collection.files.iter()
            .map(|f| (f, (None, Duration::MAX)))
            .collect::<HashMap<_, _>>();

        // Match 1:1 the files.
        for iiq in key_collection.files.iter() {
            let closest_other_file = other_collection.get_closest_file_by_datetime(&iiq.datetime)?;
            let dt = iiq.abs_diff(&closest_other_file.datetime);

            let v = join_hash.get_mut(&closest_other_file);
            let (existing_match, existing_dt) = v.unwrap();
            if dt < *existing_dt {
                *existing_match = Some(iiq);
                *existing_dt = dt;
            }
        }

        // Turn the hashmap into a vector
        let mut joined: Vec<(Option<&IIQFile>, Option<&IIQFile>, Duration)> = join_hash
            .into_iter()
            .map(|(k, (v, dt))| (Some(k), v, dt))
            .collect();

        if rgb_shorter {
            // Reverse tuples, so that order is (rgb, nir)
            joined = joined.into_iter().map(|(nir, rgb, dt)| (rgb, nir, dt)).collect();
        }

        Ok(JoinedIIQCollection { joined })
    }

    fn len(&self) -> usize {
        self.joined.len()
    }

    fn get_matched(&self, max_dt: &Duration) -> Vec<(&IIQFile, &IIQFile)> {
        self.joined
            .iter()
            .filter(|(rgb, nir, dt)| {
                rgb.is_some() && nir.is_some() && dt <= max_dt
            })
            .map(|(rgb, nir, _)| (rgb.unwrap(), nir.unwrap()))
            .collect()
    }

    fn get_matched_rgb(&self, max_dt: &Duration) -> IIQCollection {
        self.get_matched(max_dt).iter()
            .map(|(rgb, _)| (*rgb).clone())
            .collect::<Vec<IIQFile>>().into()
    }

    fn get_matched_nir(&self, max_dt: &Duration) -> IIQCollection {
        self.get_matched(max_dt).iter()
            .map(|(_, nir)| (*nir).clone())
            .collect::<Vec<IIQFile>>().into()
    }

    fn get_unmatched(&self, max_dt: &Duration) -> Vec<(Option<&IIQFile>, Option<&IIQFile>)> {
        self.joined
            .iter()
            .filter(|(rgb, nir, dt)| {
                (rgb.is_none() || nir.is_none()) || dt > max_dt
            })
            .map(|(rgb, nir, _)| (*rgb, *nir))
            .collect()
    }

    fn get_unmatched_rgb(&self, max_dt: &Duration) -> IIQCollection {
        self.get_unmatched(max_dt).iter()
            .filter(|(rgb, _)| rgb.is_some())
            .map(|(rgb, _)| (*rgb).unwrap().clone())
            .collect::<Vec<IIQFile>>().into()
    }

    fn get_unmatched_nir(&self, max_dt: &Duration) -> IIQCollection {
        self.get_unmatched(max_dt).iter()
            .filter(|(_, nir)| nir.is_some())
            .map(|(_, nir)| (*nir).unwrap().clone())
            .collect::<Vec<IIQFile>>().into()
    }
}


pub fn process_images(
    rgb_dir: &Path,
    nir_dir: &Path,
    match_threshold: Duration,
    keep_empty_files: bool,
    dry_run: bool,
    verbose: bool,
) -> Result<(usize, usize, usize, usize, usize)> {
    // Check that the directories exist
    let rgb_exists = rgb_dir.exists();
    let nir_exists = nir_dir.exists();
    if !rgb_exists || !nir_exists {
        return Err(anyhow::anyhow!("RGB and NIR directories do not exist"));
    } else if !rgb_exists {
        return Err(anyhow::anyhow!("RGB directory does not exist"));
    } else if !nir_exists {
        return Err(anyhow::anyhow!("NIR directory does not exist"));
    }

    // Find IIQ files
    let rgb_iiq_files = filesystem::find_files(rgb_dir, ".iiq")?;
    let nir_iiq_files = filesystem::find_files(nir_dir, ".iiq")?;

    // Create collections
    let mut rgb_collection = IIQCollection::new(&rgb_iiq_files)?;
    let mut nir_collection = IIQCollection::new(&nir_iiq_files)?;

    // Get 0 byte file counts
    let empty_rgb_files_len = rgb_collection.empty_files_len();
    let empty_nir_files_len = nir_collection.empty_files_len();

    if !keep_empty_files && !dry_run {
        // Move empty files
        let empty_rgb_files = rgb_collection.pop_empty_files();
        let empty_nir_files = nir_collection.pop_empty_files();

        if empty_rgb_files.len() > 0 {
            let empty_rgb_dir = rgb_dir.join("empty");
            if verbose {
                println!("Moving empty RGB files to {:?}", empty_rgb_dir);
            }
            fs::create_dir_all(&empty_rgb_dir)?;
            filesystem::move_files(empty_rgb_files.paths(), &empty_rgb_dir, verbose)?;
        }

        if empty_nir_files.len() > 0 {
            let empty_nir_dir = nir_dir.join("empty");
            if verbose {
                println!("Moving empty NIR files to {:?}", empty_nir_dir);
            }
            fs::create_dir_all(&empty_nir_dir)?;
            filesystem::move_files(empty_nir_files.paths(), &empty_nir_dir, verbose)?;
        }
    }

    // Do the join
    let joined = JoinedIIQCollection::new(&rgb_collection, &nir_collection)?;

    let matched_rgb = joined.get_matched_rgb(&match_threshold);
    let matched_nir = joined.get_matched_nir(&match_threshold);
    let unmatched_rgb = joined.get_unmatched_rgb(&match_threshold);
    let unmatched_nir = joined.get_unmatched_nir(&match_threshold);

    if !dry_run {
        // Move all matched iiq files to camera dirs root
        filesystem::move_files(matched_rgb.paths(), rgb_dir, verbose)?;
        filesystem::move_files(matched_nir.paths(), nir_dir, verbose)?;

        // Move unmatched files
        if unmatched_rgb.len() > 0 {
            let unmatched_rgb_dir = rgb_dir.join("unmatched");
            if verbose {
                println!("Moving unmatched RGB files to {:?}", unmatched_rgb_dir);
            }
            fs::create_dir_all(&unmatched_rgb_dir)?;
            filesystem::move_files(unmatched_rgb.paths(), &unmatched_rgb_dir, verbose)?;
        }
        if unmatched_nir.len() > 0 {
            let unmatched_nir_dir = nir_dir.join("unmatched");
            if verbose {
                println!("Moving unmatched NIR files to {:?}", unmatched_nir_dir);
            }
            fs::create_dir_all(&unmatched_nir_dir)?;
            filesystem::move_files(unmatched_nir.paths(), &unmatched_nir_dir, verbose)?;
        }
    }

    Ok((rgb_iiq_files.len(), nir_iiq_files.len(), matched_rgb.len(), empty_rgb_files_len, empty_nir_files_len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use tempfile::TempDir;

    use std::fs;

    #[test]
    fn test_iiq_file_new() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("210101_120000000.iiq");
        fs::write(&path, "content").unwrap();

        let file = IIQFile::new(&path).unwrap();
        assert_eq!(file.stem, "210101_120000000");
        let date = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap();
        let time = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let dt = NaiveDateTime::new(date, time);
        assert_eq!(file.datetime, dt);
        assert_eq!(file.bytes, 7);
        assert_eq!(file.path, path);
        assert_eq!(file.name, "210101_120000000.iiq");
    }

    #[test]
    fn test_make_iiq_collection() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        let files = vec![
            base_path.join("210101_120000000.iiq"),
            base_path.join("210101_120001000.iiq"),
        ];

        files.iter().for_each(|file| {
            fs::write(file, "content").unwrap();
        });

        let collection = IIQCollection::new(&files).unwrap();
        assert_eq!(collection.len(), 2);
        assert_eq!(collection.paths(), files);
    }

    #[test]
    fn test_join_collections() {
        let temp_dir_rgb = TempDir::new().unwrap();
        let rgb_files = vec![
            temp_dir_rgb.path().join("210101_120000000.iiq"),
            temp_dir_rgb.path().join("210101_120001000.iiq"),
        ];
        for file in &rgb_files {
            fs::write(file, "content").unwrap();
        }
        let rgb_collection = IIQCollection::new(&rgb_files).unwrap();

        let temp_dir_nir = TempDir::new().unwrap();
        let nir_files = vec![
            temp_dir_nir.path().join("210101_120000100.iiq"),
            temp_dir_nir.path().join("210101_120001100.iiq"),
        ];
        for file in &nir_files {
            fs::write(file, "content").unwrap();
        }
        let nir_collection = IIQCollection::new(&nir_files).unwrap();

        let result = JoinedIIQCollection::new(&rgb_collection, &nir_collection).unwrap();

        assert_eq!(result.len(), 2);
        let mut joined = result.joined;
        joined.sort();
        assert_eq!(joined, vec![
            (Some(&rgb_collection.files[0]), Some(&nir_collection.files[0]), Duration::from_millis(100)),
            (Some(&rgb_collection.files[1]), Some(&nir_collection.files[1]), Duration::from_millis(100)),
        ]);
    }

    #[test]
    fn test_collection() {
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

        let rgb_files = filesystem::find_files(&rgb_dir, ".iiq").unwrap();
        let nir_files = filesystem::find_files(&nir_dir, ".iiq").unwrap();

        let rgb_collection = IIQCollection::new(&rgb_files).unwrap();
        let nir_collection = IIQCollection::new(&nir_files).unwrap();

        assert_eq!(rgb_collection.len(), 2);
        assert_eq!(nir_collection.len(), 2);
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
        let (rgb_count, nir_count, matched_count, empty_rgb_count, empty_nir_count) =
            process_images(&rgb_dir, &nir_dir, threshold, false, false, false).unwrap();

        assert_eq!(rgb_count, 2);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 2);
        assert_eq!(empty_rgb_count, 0);
        assert_eq!(empty_nir_count, 0);

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
        let (rgb_count, nir_count, matched_count, empty_rgb_count, empty_nir_count) =
            process_images(&rgb_dir, &nir_dir, threshold, true, true, false).unwrap();

        assert_eq!(rgb_count, 2);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 1);
        assert_eq!(empty_rgb_count, 0);
        assert_eq!(empty_nir_count, 0);

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
        let (rgb_count, nir_count, matched_count, empty_rgb_count, empty_nir_count) =
            process_images(&rgb_dir, &nir_dir, threshold, true, false, false).unwrap();

        assert_eq!(rgb_count, 2);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 1);
        assert_eq!(empty_rgb_count, 0);
        assert_eq!(empty_nir_count, 0);

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
        let (rgb_count, nir_count, matched_count, empty_rgb_count, empty_nir_count) =
            process_images(&rgb_dir, &nir_dir, threshold, true, false, false).unwrap();

        assert_eq!(rgb_count, 1);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 1);
        assert_eq!(empty_rgb_count, 0);
        assert_eq!(empty_nir_count, 0);

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

    #[test]
    fn test_process_images_with_no_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let rgb_dir = temp_dir.path().join("rgb");
        let nir_dir = temp_dir.path().join("nir");

        let threshold = Duration::from_millis(200);
        let result = process_images(&rgb_dir, &nir_dir, threshold, true, false, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_process_images_with_keep_empty() {
        let temp_dir = TempDir::new().unwrap();
        let rgb_dir = temp_dir.path().join("rgb");
        let nir_dir = temp_dir.path().join("nir");
        fs::create_dir_all(&rgb_dir).unwrap();
        fs::create_dir_all(&nir_dir).unwrap();

        // Create test files
        fs::write(rgb_dir.join("210101_120000000.iiq"), "content").unwrap();
        fs::write(rgb_dir.join("210101_130000000.iiq"), "").unwrap();
        fs::write(nir_dir.join("210101_120000100.iiq"), "content").unwrap();
        fs::write(nir_dir.join("210101_130000100.iiq"), "").unwrap();

        let threshold = Duration::from_millis(200);
        let (rgb_count, nir_count, matched_count, empty_rgb_count, empty_nir_count) =
            process_images(&rgb_dir, &nir_dir, threshold, true, false, false).unwrap();

        assert_eq!(rgb_count, 2);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 2);
        assert_eq!(empty_rgb_count, 1);
        assert_eq!(empty_nir_count, 1);

        // Check if matched files are in their original locations
        assert!(rgb_dir.join("210101_120000000.iiq").exists());
        assert!(nir_dir.join("210101_120000100.iiq").exists());
        assert!(rgb_dir.join("210101_130000000.iiq").exists());
        assert!(nir_dir.join("210101_130000100.iiq").exists());

        // Check that no empty directories were created
        assert!(!rgb_dir.join("empty").exists());
        assert!(!nir_dir.join("empty").exists());
    }

    #[test]
    fn test_process_images_with_no_keep_empty() {
        let temp_dir = TempDir::new().unwrap();
        let rgb_dir = temp_dir.path().join("rgb");
        let nir_dir = temp_dir.path().join("nir");
        fs::create_dir_all(&rgb_dir).unwrap();
        fs::create_dir_all(&nir_dir).unwrap();

        // Create test files
        fs::write(rgb_dir.join("210101_120000000.iiq"), "content").unwrap();
        fs::write(rgb_dir.join("210101_130000000.iiq"), "").unwrap();
        fs::write(nir_dir.join("210101_120000100.iiq"), "content").unwrap();
        fs::write(nir_dir.join("210101_130000100.iiq"), "").unwrap();

        let threshold = Duration::from_millis(200);
        let (rgb_count, nir_count, matched_count, empty_rgb_count, empty_nir_count) =
            process_images(&rgb_dir, &nir_dir, threshold, false, false, false).unwrap();

        assert_eq!(rgb_count, 2);
        assert_eq!(nir_count, 2);
        assert_eq!(matched_count, 1);
        assert_eq!(empty_rgb_count, 1);
        assert_eq!(empty_nir_count, 1);

        // Check if matched files are in their original locations
        assert!(rgb_dir.join("210101_120000000.iiq").exists());
        assert!(nir_dir.join("210101_120000100.iiq").exists());

        // Check if empty files are moved to the empty directory
        assert!(rgb_dir.join("empty").join("210101_130000000.iiq").exists());
        assert!(!rgb_dir.join("210101_130000000.iiq").exists());
        assert!(nir_dir.join("empty").join("210101_130000100.iiq").exists());
        assert!(!nir_dir.join("210101_130000100.iiq").exists());
    }

    #[test]
    fn test_get_closest_file_by_datetime() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        let files = vec![
            base_path.join("210101_120000000.iiq"),
            base_path.join("210101_120001000.iiq"),
            base_path.join("210101_120002000.iiq"),
        ];

        files.iter().for_each(|file| {
            fs::write(file, "content").unwrap();
        });

        let collection = IIQCollection::new(&files).unwrap();

        let target_datetime = NaiveDateTime::parse_from_str("210101_120000500", "%y%m%d_%H%M%S%3f").unwrap();
        let closest_file = collection.get_closest_file_by_datetime(&target_datetime).unwrap();
        assert_eq!(closest_file.path, files[0]);

        let target_datetime = NaiveDateTime::parse_from_str("210101_120001500", "%y%m%d_%H%M%S%3f").unwrap();
        let closest_file = collection.get_closest_file_by_datetime(&target_datetime).unwrap();
        assert_eq!(closest_file.path, files[1]);

        let target_datetime = NaiveDateTime::parse_from_str("210101_120002500", "%y%m%d_%H%M%S%3f").unwrap();
        let closest_file = collection.get_closest_file_by_datetime(&target_datetime).unwrap();
        assert_eq!(closest_file.path, files[2]);
    }

    #[test]
    fn test_get_closest_file_by_datetime_empty_collection() {
        let collection = IIQCollection { files: vec![] };
        let target_datetime = NaiveDateTime::parse_from_str("210101_120000500", "%y%m%d_%H%M%S%3f").unwrap();
        let result = collection.get_closest_file_by_datetime(&target_datetime);
        assert!(result.is_err());
    }
}
