import datetime  # noqa: D100
import json
import sys
from http import HTTPStatus
from pathlib import Path

import click
import polars as pl
import requests
from packaging import version
from requests.exceptions import (
    ConnectionError,  # noqa: A004
    HTTPError,
    RequestException,
    Timeout,
)

CACHE_DIR = ".sisyphus"         # Папка в домашнем каталоге для хранения файлов
TTL_IN_HOURS = 1                # Время актуальности кэша в часах
TTL = 60 * 60 * TTL_IN_HOURS    # Время актуальности кэша в секундах

branch_name = "sisyphus"


def get_cache_dir_path() -> Path:
    """Return the path to the folder where downloaded files are stored.

    :return: Path to the folder.
    """
    return Path.home() / CACHE_DIR

def get_file_name(branch: str) -> str:
    """Return the name of the file where JSON should be stored.

    :param branch: Alt Linux branch.
    :return: File name.
    """
    return f"{branch}.json"

def get_branch_file_name(branch: str) -> Path:
    """Return the full path to the file storing JSON, including its name.

    :param branch: Alt Linux branch.
    :return: Full path to the file, including its name.
    """
    return get_cache_dir_path() / get_file_name(branch)

def is_branch_cache_exists(branch: str) -> bool:
    """Check if the file storing JSON exists.

    :param branch: Alt Linux branch.
    :return: `True` if the file exists.
    """
    return get_branch_file_name(branch).exists()


def delete_file(file: Path) -> bool:
    """Delete a file. If deletion fails, report an error.

    :param file: Path to the file.
    :return: `True` if the file was deleted.
    """
    try:
        file.unlink()
    except FileNotFoundError:
        print(f"File not found.  Deletion of {file} aborted.")  # noqa: T201
        return False
    else:
        return True


def get_file_modification_time(file: Path) -> datetime.datetime:
    """Возвращает время модификации файла.

    :param file: Путь к файлу.
    :return: Время модификации файла.
    """
    return datetime.datetime.fromtimestamp(file.stat().st_mtime, tz=datetime.UTC)


def get_file_creation_time(file: Path) -> datetime.datetime:
    """Return the creation time of a file.

    :param file: Path to the file.
    :return: Creation time of the file.
    """
    return datetime.datetime.fromtimestamp(file.stat().st_ctime, tz=datetime.UTC)


def is_cached(branch: str) -> bool:
    """Check if a valid cache file exists for the specified distribution branch.

    :param branch: Alt Linux branch.
    :return: `True` if a valid cache file exists.
    """
    if not is_branch_cache_exists(branch):
        return False
    now_time = datetime.datetime.now(tz=datetime.UTC)
    file_age = now_time - get_file_creation_time(get_branch_file_name(branch))
    return int(file_age.total_seconds()) <= TTL


def create_cache_dir(cache_dir: Path|None = None) -> bool:
    """Create the directory where cache files should be stored.

    :param cache_dir: Directory for storing cache files.
    :return: `True` if the directory was created.
    """
    cache_dir = cache_dir if cache_dir else get_cache_dir_path()
    if cache_dir.exists():
        return True
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir.exists()


def get_branch_json(branch: str) -> Path|None:
    """Return the path to the cache file corresponding to the distribution branch.

    :param branch: Alt Linux branch.
    :return: Path to the cache file if it exists or was successfully created.
    """
    json_file = get_branch_file_name(branch)
    if not is_cached(branch):
        if json_file.exists():
            if not delete_file(json_file):
                return None
        else:
            create_cache_dir()
        # загрузка файла
        print(f"Downloading json data for {branch}...")
        json_file = download(
            f"https://rdb.altlinux.org/api/export/branch_binary_packages/{branch}",
            json_file,
        )
    return json_file


def download(url: str, filename: Path) -> Path|None:
    """Downloads data and saves it to a file.

    :param url: URL where the data can be found.
    :param filename: Path to the file where the downloaded data should be saved.
    :return: Path to the file containing the downloaded data,
         or `None` if the download failed.
    """  # noqa: D401
    try:
        response = requests.get(url, stream=True, timeout=5)
        response.raise_for_status()
    except ConnectionError as e:
        print("Connection Error. Make sure you are connected to Internet.\n")  # noqa: T201
        print(str(e)) # noqa: T201
        return None
    except Timeout as e:
        print("Timeout Error.") # noqa: T201
        print(str(e)) # noqa: T201
        return None
    except HTTPError as e:
        print("HTTP Error.") # noqa: T201
        print(str(e)) # noqa: T201
        status_code = e.response.status_code
        if status_code == HTTPStatus.BAD_REQUEST:
            print("Request parameters validation error.") # noqa: T201
        elif status_code == HTTPStatus.NOT_FOUND:
            print("Requested data not found in database.") # noqa: T201
        return None
    except RequestException as e:
        print("General Error.") # noqa: T201
        print(str(e)) # noqa: T201
        return None
    except KeyboardInterrupt:
        print("Someone closed the program") # noqa: T201
        return None
    else:
        total = response.headers.get("content-length")
        try:
            with filename.open("wb") as f:
                if total is None:
                    f.write(response.content)
                else:
                    downloaded = 0
                    total = int(total)
                    for data in response.iter_content(chunk_size=max(int(total/1000), 1024*1024)):  # noqa: E501
                        downloaded += len(data)
                        f.write(data)
                        done = int(50*downloaded/total)
                        sys.stdout.write("\r[{}{}]".format("█" * done, "." * (50-done)))
                        sys.stdout.flush()
                f.flush()
            sys.stdout.write("\n")
        except PermissionError as e:
            sys.stdout.write("\n")
            print("Permission Error.") # noqa: T201
            print(str(e)) # noqa: T201
            if filename.exists():
                delete_file(filename)
            return None
        except KeyboardInterrupt:
            sys.stdout.write("\n")
            print("Someone closed the program") # noqa: T201
            if filename.exists():
                delete_file(filename)
            return None
        except OSError as e:
            sys.stdout.write("\n")
            print("OS Error.") # noqa: T201
            print(str(e)) # noqa: T201
            if filename.exists():
                delete_file(filename)
            return None
        except Exception as e:  # noqa: BLE001
            sys.stdout.write("\n")
            print("General Error.") # noqa: T201
            print(str(e)) # noqa: T201
            if filename.exists():
                delete_file(filename)
            return None
        else:
            return filename


def safe_version(v: str) -> str:
    """Normalize version string for comparison.

    :param v: Input version string.
    :return: Normalized version string, using `packaging.version.parse` if possible,
             otherwise falling back to zero-padded numeric parts and lowercase text.
    """
    try:
        return str(version.parse(v))
    except version.InvalidVersion:
        parts = v.split(".")
        normalized = []
        for p in parts:
            if p.isdigit():
                normalized.append(f"{int(p):08d}")
            else:
                normalized.append(p.lower())
        return ".".join(normalized)

# ########################################################################

@click.command()
@click.argument("branch1", type=click.STRING)
@click.argument("branch2", type=click.STRING)
@click.option(
    "--force",
    "-f",
    help="Force overwrite of existing branches data.",
    is_flag=True,
    default=False,
)
def main(branch1: str, branch2: str, force: bool): # noqa: FBT001
    """Программа, сравнивающая пакеты в двух разных ветках Alt Linux.

    BRANCH1, BRANCH2 - имена веток.

    Пример:  sisyphus.py sisyphus p11
    """
    if force:
        click.echo("Forcing overwrite of existing branches data.")
        if is_branch_cache_exists(branch1):
            delete_file(get_branch_file_name(branch1))
        if is_branch_cache_exists(branch2):
            delete_file(get_branch_file_name(branch2))
    json1 = get_branch_json(branch1)
    click.echo(f"JSON file for branch name {branch1} is {json1}.")
    if json1:
        json2 = get_branch_json(branch2)
        click.echo(f"JSON file for branch name {branch2} is {json2}.")
        if not json2:
            click.echo(f"Don't know how to get JSON file for branch name {branch2}.")
    else:
        click.echo(f"Don't know how to get JSON file for branch name {branch1}.")
    if json1 and json2:
        first = pl.read_json(json1).select("packages").explode("packages").unnest("packages")
        second = pl.read_json(json2).select("packages").explode("packages").unnest("packages")

        archs = second["arch"].unique().append(first["arch"].unique()).unique()

        for arch in list(archs):
            first_by_arch = first.filter(first["arch"] == arch)
            second_by_arch = second.filter(second["arch"] == arch)

            second_only = second_by_arch.join(
                first_by_arch, on="name", how="anti",
            )  # первое задание
            first_only = first_by_arch.join(
                second_by_arch, on="name", how="anti",
            )  # второе задание

            equal_packages = second_by_arch.join(
                first_by_arch, on="name", how="inner",
            ).select("name", first_branch_version="version_right",
                     second_branch_version="version", arch="arch")

            result = equal_packages.filter(
                pl.col("first_branch_version").map_elements(
                    safe_version, return_dtype=pl.Utf8)
                > pl.col("second_branch_version").map_elements(
                    safe_version, return_dtype=pl.Utf8),
            )  # третье задание

            json_data = {}

            json_data[arch] = {
                "second_only_count": len(second_only),
                "second_only": second_only.to_dicts(),
                "first_only_count": len(first_only),
                "first_only": first_only.to_dicts(),
                "newer_in_first_count": len(result),
                "newer_in_first": result.to_dicts(),
            }
        try:
            with open(f"{get_cache_dir_path()}/data.json", "w+", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            click.echo(f"Result downloaded in {get_cache_dir_path()}/data.json.")
        except Exception:
            click.echo("Failed to write calculated data.")




if __name__ == "__main__":
    main()
