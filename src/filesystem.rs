use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use globwalker::{FileType, GlobWalkerBuilder};

pub fn find_dir_by_pattern(
    base_dir: &PathBuf,
    dir_pattern: &str,
    case_sensitive: bool,
) -> Option<PathBuf> {
    let walker = GlobWalkerBuilder::from_patterns(base_dir, &[dir_pattern])
        .case_insensitive(!case_sensitive)
        .follow_links(true)
        .max_depth(1)
        .file_type(FileType::DIR)
        .build()
        .expect("Failed to create glob walker");

    let mut dirs: Vec<_> = walker
        .filter_map(Result::ok)
        .map(|entry| entry.into_path())
        .collect();

    match dirs.len() {
        1 => dirs.pop(),
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

pub fn find_files(base_dir: &Path, extension: &str) -> Result<Vec<PathBuf>> {
    let pattern = format!("**/*.{}", extension);

    let walker = GlobWalkerBuilder::from_patterns(base_dir, &[pattern])
        .follow_links(true)
        .file_type(FileType::FILE)
        .build()
        .expect("Failed to create glob walker");

    let files: Vec<_> = walker
        .filter_map(Result::ok)
        .map(|entry| entry.into_path())
        .collect();

    Ok(files)
}

pub fn move_files(paths: Vec<PathBuf>, dir: &Path, verbose: bool) -> Result<()> {
    // Move files to 'unmatched' directory
    for path in paths {
        let dest = dir.join(
            path.file_name()
                .context("Failed to get file destination name")?,
        );
        if verbose {
            println!("{} -> {}", path.display(), dest.display());
        }
        fs::rename(&path, &dest)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_find_dir_by_pattern() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        fs::create_dir(base_path.join("test_dir_123")).unwrap();
        fs::create_dir(base_path.join("another_dir_456")).unwrap();

        let result = find_dir_by_pattern(&base_path, "test_dir_*", true);
        assert!(result.is_some());
        assert_eq!(result.unwrap().file_name().unwrap(), "test_dir_123");

        let no_match = find_dir_by_pattern(&base_path, "nonexistent_*", true);
        assert!(no_match.is_none());

        fs::create_dir(base_path.join("CAMERA_RGB")).unwrap();
        let result = find_dir_by_pattern(&base_path, "C*_RGB", true);
        assert!(result.is_some());
        assert_eq!(result.unwrap().file_name().unwrap(), "CAMERA_RGB");

        fs::create_dir(base_path.join("camera_nir")).unwrap();
        let result = find_dir_by_pattern(&base_path, "CAMERA_NIR", true);
        assert!(result.is_none());
        let result = find_dir_by_pattern(&base_path, "CAMERA_NIR", false);
        assert!(result.is_some());
        assert_eq!(result.unwrap().file_name().unwrap(), "camera_nir");
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
    fn test_move_files() {
        let temp_dir = TempDir::new().unwrap();
        let source_dir = temp_dir.path().join("source");
        let dest_dir = temp_dir.path().join("dest");
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&dest_dir).unwrap();

        let paths = vec![source_dir.join("file1.txt"), source_dir.join("file2.txt")];

        // Create test files
        for path in &paths {
            fs::write(path, "content").unwrap();
        }

        move_files(paths, &dest_dir, false).unwrap();

        assert!(!source_dir.join("file1.txt").exists());
        assert!(!source_dir.join("file2.txt").exists());
        assert!(dest_dir.join("file1.txt").exists());
        assert!(dest_dir.join("file2.txt").exists());
    }
}
