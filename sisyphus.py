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
    """Return the modification time of a file.

    :param file: Path to the file.
    :return: Modification time of the file.
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


def get_branch_json(branch: str, arch: str = "all") -> Path|None:
    """Return the path to the cache file corresponding to the distribution branch.

    :param branch: Alt Linux branch.
    :param arch: Alt Linux architecture.
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
        if arch == "all":
            json_file = download(
                f"https://rdb.altlinux.org/api/export/branch_binary_packages/{branch}",
                json_file,
            )
        else:
            json_file = download(
                f"https://rdb.altlinux.org/api/export/branch_binary_packages/{branch}?arch={arch}",
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


def safe_version(version_name: str) -> str:
    """Normalize version string for comparison.

    :param version_name: Input version string.
    :return: Normalized version string, using `packaging.version.parse` if possible,
             otherwise falling back to zero-padded numeric parts and lowercase text.
    """
    parts = version_name.split(".")
    normalized = []
    for p in parts:
        if p.isdigit():
            normalized.append(f"{int(p):08d}")
        else:
            normalized.append(p.lower())
    return ".".join(normalized)


def compare(m: str, n: str) -> str:
    """Compare two versions.

    :param m: First version string.
    :param n: Second version string.
    :return: Symbol of comparison between `m` and `n`.
    """
    def strange_compare(m: str, n: str) -> str:
        v1_srange = safe_version(m)
        v2_strange = safe_version(n)
        if v1_srange < v2_strange:
            return "<"
        return ">"
    if m == n:
        return "="
    try:
        v1 = version.parse(m)
        v2 = version.parse(n)
    except ValueError:
        return strange_compare(m, n)
    if v1 < v2:
        return "<"
    return ">"

def get_filter_expression(parameter: str) -> pl.Expr:
        """Get polars expression by comparison symbol.

        :param parameter: Comparison parameter, e.g. > or <.
        :return: Polars expression for this parameter.
        """
        ex_unknown = pl.col("compare")=="?"
        ex_less = pl.col("compare")=="<"
        ex_great = pl.col("compare")==">"
        ex_equal = pl.col("compare")=="="
        ex_great_or_equal = ex_great | ex_equal

        filter_expression = ex_unknown
        if parameter == "?":
            filter_expression = ex_unknown
        elif parameter == "=":
            filter_expression = ex_equal
        elif parameter == "<":
            filter_expression = ex_less
        elif parameter == ">":
            filter_expression = ex_great
        elif parameter == ">=":
            filter_expression = ex_great_or_equal
        return filter_expression



# ########################################################################

@click.command()
@click.argument("branch1", type=click.STRING)
@click.argument("branch2", type=click.STRING)
@click.option("--arch", "-a",
              default="all",
              help="Only one architecture. Example: --arch aarch64"
)
@click.option(
    "--force",
    "-f",
    help="Force overwrite of existing branches data.",
    is_flag=True,
    default=False,
)
@click.option(
    "--comp",
    "-c",
    type=click.Choice(["?", "<", ">", "=", ">="]),
    help="How to compare two branches versions.",
    default=">",
)
def main(branch1: str, branch2: str, force: bool, arch: str, comp: str) -> None: # noqa: FBT001
    """Compare packages in two different Alt Linux branches.

    BRANCH1, BRANCH2 - names of the branches.
    Example:  sisyphus.py sisyphus p11
    """
    json2 = None
    if force:
        click.echo("Forcing overwrite of existing branches data.")
        if is_branch_cache_exists(branch1):
            delete_file(get_branch_file_name(branch1))
        if is_branch_cache_exists(branch2):
            delete_file(get_branch_file_name(branch2))
    json1 = get_branch_json(branch1, arch)

    if json1:
        click.echo(f"JSON file for branch name {branch1} is {json1}.")
        json2 = get_branch_json(branch2, arch)
        if json2:
            click.echo(f"JSON file for branch name {branch2} is {json2}.")
        else:
            click.echo(f"Don't know how to get JSON file for branch name {branch2}.")
            return
    else:
        click.echo(f"Don't know how to get JSON file for branch name {branch1}.")
        return

    col = "packages"
    first_branch_packages = pl.read_json(json1).select(col).explode(col).unnest(col)
    second_branch_packages = pl.read_json(json2).select(col).explode(col).unnest(col)

    architectures = (
        second_branch_packages["arch"].unique()
        .append(
            first_branch_packages["arch"].unique()
        ).unique()
    )
    json_data = {}
    for sys_arch in list(architectures):
        first_by_arch = first_branch_packages.filter(first_branch_packages["arch"] == sys_arch)  # noqa: E501
        second_by_arch = second_branch_packages.filter(second_branch_packages["arch"] == sys_arch)  # noqa: E501

        # Первая задача:
        # Выбрать из списка пакетов все пакеты, которые есть
        # во второй ветке дистрибутива, но нет в первой.
        second_only_packages = second_by_arch.join(first_by_arch, on="name", how="anti")

        # Вторая задача:
        # Выбрать из списка пакетов все пакеты, которые есть
        # в первой ветке дистрибутива, но нет во второй.
        first_only_packages = first_by_arch.join(second_by_arch, on="name", how="anti")

        # Третья задача:
        # Выбрать все пакеты из первой ветки дистрибутива, версии которых больше
        # чем версии тех же пакетов второй ветки дистрибутива.

        equal_packages = (
            second_by_arch.join(first_by_arch, on="name", how="inner")
            .select(
                "name",
                first_branch_version="version_right",
                second_branch_version="version",
            )
            .with_columns(
                pl.struct("first_branch_version", "second_branch_version")
                .map_elements(
                    lambda s: compare(
                        s["first_branch_version"], s["second_branch_version"]
                    ),
                    return_dtype=pl.String,
                )
                .alias("compare")
            )
            .filter(get_filter_expression(comp))
            .select(pl.all().exclude("compare"))
        )

        json_data[arch] = {
            "second_only_count": len(second_only_packages),
            "second_only_packages": second_only_packages.to_dicts(),
            "first_only_count": len(first_only_packages),
            "first_only_packages": first_only_packages.to_dicts(),
            "newest_in_first_count": len(equal_packages),
            "newest_in_first": equal_packages.to_dicts(),
        }

    try:
        output_file_name = f"{get_cache_dir_path()}/output.json"
        output_file = Path(output_file_name)
        with output_file.open("w+", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
            f.flush()


        click.echo(f"Result downloaded in {output_file_name}.")
    except Exception:
        click.echo("Failed to write calculated data.")


if __name__ == "__main__":
    main()
