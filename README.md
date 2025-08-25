# Alt Linux Branch Packages Comparator

This Python module and CLI tool compares binary packages between two Alt Linux branches using the public REST API:

```
https://rdb.altlinux.org/api/export/branch_binary_packages/{branch}
```

It produces a JSON report sorted by architecture.

## Features

- Download package lists for two branches (e.g., `sisyphus` and `p11`)
- Compare packages and output:
  - Packages present only in second branch
  - Packages present only in first branch
  - Packages with newer version-release in first branch (e.g. `sisyphus`)
- Caches downloaded data to reduce repeated API calls
- JSON output structured by architecture

## Installation

1. Clone the repository:

```bash
git clone <repository_url>
cd <repository_directory>
```

2. Install dependencies:

```bash
uv sync
```

Dependencies:  
- `click`  
- `polars`  
- `requests`  
- `packaging`

## Usage

Run the CLI tool with two branch names:

```bash
uv run main.py <branch1> <branch2>
```

Example:

```bash
uv run main.py sisyphus p11
```

### Options

- `--force` / `-f` : Force overwrite of cached data

Example:

```bash
uv run main.py sisyphus p11 --force
```

## Output

The JSON file is saved at:

```
~/.sisyphus/data.json
```

Example structure:

```json
{
  "x86_64": {
    "second_only_count": 10,
    "second_only": [...],
    "first_only_count": 5,
    "first_only": [...],
    "newer_in_first_count": 3,
    "newer_in_first": [...]
  },
  "i586": {
    ...
  }
}
```

- `second_only`: packages present in the second branch but missing in the first  
- `first_only`: packages present in the first branch but missing in the second  
- `newer_in_first`: packages whose version-release is newer in the first branch  

## Cache

- Cached files are stored in `~/.sisyphus/`
- Cache is valid for 1 hour
- Cache is automatically refreshed if expired or if `--force` is used

## Notes

- Version comparison follows RPM versioning rules using `packaging.version.parse`
- Tested on ALT Linux 11
- More information on branches and packages: [Alt Linux Packages](https://packages.altlinux.org/ru/sisyphus/)
