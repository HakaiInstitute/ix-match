# IX-Match

IX-Match is a Rust CLI tool and library for matching RGB and NIR IIQ files from aerial surveys using PhaseOne cameras.
It helps preprocess images for conversion with IX-Capture by moving unmatched images to a new subdirectory.

## Features

- Matches RGB and NIR IIQ files based on timestamps
- Moves unmatched files to separate directories
- Configurable matching threshold
- Dry-run option for testing without moving files
- Verbose output for detailed information

## Installation

To install IX-Match, you need to have Rust and Cargo installed on your system. If you don't have them installed, you can
get them from [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install).

Once you have Rust and Cargo installed, you can install IX-Match using:

```
cargo install ix-match
```

## Usage

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
   cargo run -- [OPTIONS] [IIQ_DIR]
   ```

6. To create a release build:
   ```
   cargo build --release
   ```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.