# IX-Match

IX-Match is a Rust CLI tool and library for matching RGB and NIR IIQ files from aerial surveys using PhaseOne cameras.
It helps preprocess images for conversion with IX-Capture by moving unmatched images to a new subdirectory.

## Features

- Matches RGB and NIR IIQ files based on timestamps
- Moves unmatched files to separate directories
- Configurable matching threshold
- Dry-run option for testing without moving files
- Verbose output for detailed information
- Can be used as a library or a CLI tool

## Installation

To install IX-Match, you need to have Rust and Cargo installed on your system. If you don't have them installed, you can
get them from [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install).

### As a CLI tool

To install IX-Match as a CLI tool, use:

```
cargo install ix-match --features cli
```

### As a library

To use IX-Match as a library in your Rust project, add the following to your `Cargo.toml`:

```toml
[dependencies]
ix-match = "0.2.4"
```

## Usage

### CLI Usage

```
ix-match [OPTIONS] [IIQ_DIR]
```

Arguments:

- `IIQ_DIR`: Directory containing the RGB and NIR subdirectories (default: current directory)

Options:

- `-d, --dry-run`: Perform a dry run without moving files
- `-r, --rgb-pattern <RGB_PATTERN>`: Pattern for finding the RGB directory (default: "C*_RGB")
- `-n, --nir-pattern <NIR_PATTERN>`: Pattern for finding the NIR directory (default: "C*_NIR")
- `-t, --thresh <THRESH>`: Threshold for matching images in milliseconds (default: 500)
- `-v, --verbose`: Enable verbose output
- `-h, --help`: Print help
- `-V, --version`: Print version

### Library Usage

To use IX-Match as a library, you can import and use its functions in your Rust code:

```rust
use ix_match::{find_dir_by_pattern, process_images};
use std::path::Path;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let iiq_dir = Path::new("path/to/iiq/directory");
    let rgb_dir = find_dir_by_pattern(iiq_dir, "C*_RGB").expect("RGB directory not found");
    let nir_dir = find_dir_by_pattern(iiq_dir, "C*_NIR").expect("NIR directory not found");
    
    let thresh = Duration::from_millis(500);
    let dry_run = false;
    let verbose = false;
    
    let (rgb_count, nir_count, matched_count) = process_images(&rgb_dir, &nir_dir, thresh, dry_run, verbose)?;
    println!("RGB: {}, NIR: {} ({} match)", rgb_count, nir_count, matched_count);
    
    Ok(())
}
```

## Development

To make changes to IX-Match, follow these steps:

1. Clone the repository:
   ```
   git clone https://github.com/HakaiInstitute/ix-match.git
   cd ix-match
   ```

2. Make your changes to the source code. The main files to edit are:
   - `src/main.rs`: Contains the CLI interface and main program logic
   - `src/lib.rs`: Contains the core functionality of the library
   - `Cargo.toml`: Manage dependencies and project metadata

3. Build the project:
   ```
   cargo build
   ```

4. Run tests:
   ```
   cargo test
   ```

5. Run the program locally:
   ```
   cargo run --features cli -- [OPTIONS] [IIQ_DIR]
   ```

6. To create a release build:
   ```
   cargo build --release --features cli
   ```

Note: The `cli` feature flag is required to build and run the CLI version of IX-Match.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.