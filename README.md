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

2. Make script executable:

```bash
chmod +x sisyphus.py
```

3. Install `uv` python package manager:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

4. Add `uv` to `PATH`

```bash
source $HOME/.local/bin/env
```
For more information about uv intallation visit:
https://docs.astral.sh/uv/getting-started/installation/#installation-methods

Dependencies:  
- `click`  
- `polars`  
- `requests`  
- `packaging`

## Usage

Run the CLI tool with two branch names:

```bash
./sisyphus.py <branch1> <branch2>
```

Example:

```bash
./sisyphus.py sisyphus p11
```

### Options

- `--force` / `-f` : Force overwrite of cached data
- `--arch` / `-a` : Download only this architecture
- `--comp` / `-c` : Comparison by needed symbol

Example:

```bash
./sisyphus.py sisyphus p11 --force
./sisyphus.py sisyphus p11 --arch aarch64
./sisyphus.py sisyphus p11 --comp gt
```

## Output

The JSON file is saved at current directory:

```
./output.json
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
