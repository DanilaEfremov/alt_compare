import datetime
import json
import sys
from http import HTTPStatus
from pathlib import Path
from timeit import default_timer as timer
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

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


def compare(version_of_first_branch: str, version_of_second_branch: str) -> str:
    """Compare two versions of packages.

    :param version_of_first_branch: First version string.
    :param version_of_second_branch: Second version string.
    :return: Symbol of comparison between `version_of_first_branch` and `version_of_second_branch`.
    """  # noqa: E501
    def strange_compare(m: str, n: str) -> str:
        """Compare two versions of packages, if parent function can not perform comparison.

        :param m: First version string.
        :param n: Second version string.
        :return: Symbol of comparison between `m` and `n`.
        """  # noqa: E501
        v1_srange = safe_version(m)
        v2_strange = safe_version(n)
        if v1_srange < v2_strange:
            return "<"
        return ">"

    if version_of_first_branch == version_of_second_branch:
        return "="
    try:
        v1 = version.parse(version_of_first_branch)
        v2 = version.parse(version_of_second_branch)
    except ValueError:
        return strange_compare(version_of_first_branch, version_of_second_branch)
    if v1 < v2:
        return "<"
    elif v1 == v2:
        return "="
    return ">"


def get_filter_expression(parameter: str) -> pl.Expr:
        """Get Polars expression by comparison symbol.

        :param parameter: Comparison parameter, e.g. > or <.
        :return: Polars expression for this parameter.
        """
        ex_less = pl.col("compare")=="<"
        ex_great = pl.col("compare")==">"
        ex_equal = pl.col("compare")=="="
        ex_great_or_equal = ex_great | ex_equal
        ex_less_or_equal = ex_less | ex_equal
        ex_not_equal = ex_great | ex_less

        filter_expression = ex_great
        if parameter == "eq":
            filter_expression = ex_equal
        elif parameter == "lt":
            filter_expression = ex_less
        elif parameter == "gt":
            filter_expression = ex_great
        elif parameter == "ge":
            filter_expression = ex_great_or_equal
        elif parameter == "le":
            filter_expression = ex_less_or_equal
        elif parameter == "ne":
            filter_expression = ex_not_equal
        return filter_expression





# p9 p10 p11 sisyphus
col = "packages"

json1 = get_branch_json('p9')
json2 = get_branch_json('p10')
json3 = get_branch_json('p11')
json4 = get_branch_json('sisyphus')

first_branch_packages = pl.read_json(json1).select(col).explode(col).unnest(col).select(["name", "version", "arch"]).rename({"version": "p9_version"})
second_branch_packages = pl.read_json(json2).select(col).explode(col).unnest(col).select(["name", "version", "arch"]).rename({"version": "p10_version"})
third_branch_packages = pl.read_json(json3).select(col).explode(col).unnest(col).select(["name", "version", "arch"]).rename({"version": "p11_version"})
fourth_branch_packages = pl.read_json(json4).select(col).explode(col).unnest(col).select(["name", "version", "arch"]).rename({"version": "sisyphus_version"})

counts = {
    "p9": len(first_branch_packages),
    "p10": len(second_branch_packages),
    "p11": len(third_branch_packages),
    "sisyphus": len(fourth_branch_packages),
}

# plt.bar(counts.keys(), counts.values())
# plt.ylabel("Количество пакетов")
# plt.xlabel("Ветка")
# plt.title("Количество пакетов в каждой ветке")
# plt.show()

architectures = (
        second_branch_packages["arch"].unique()
        .append(
            first_branch_packages["arch"].unique()
        ).unique().append(
            third_branch_packages["arch"].unique()
        ).unique().append(
            fourth_branch_packages["arch"].unique()
        ).unique()
    )


counts_by_arch = {}
for arch in architectures:
    counts_by_arch[f"9{arch}"] = len(first_branch_packages.filter(first_branch_packages["arch"] == arch))
    counts_by_arch[f"10{arch}"] = len(second_branch_packages.filter(second_branch_packages["arch"] == arch))
    counts_by_arch[f"11{arch}"] = len(third_branch_packages.filter(third_branch_packages["arch"] == arch))
    counts_by_arch[f"si{arch}"] = len(third_branch_packages.filter(third_branch_packages["arch"] == arch))


# barlist = plt.bar(counts_by_arch.keys(), counts_by_arch.values())
# for i in range(0, len(barlist)):
#     if (i+1) % 4 == 0:
#         barlist[i].set_color("blue")
#     elif (i+1) % 4 == 1:
#         barlist[i].set_color("red")
#     elif (i+1) % 4 == 2:
#         barlist[i].set_color("green")
#     else:
#         barlist[i].set_color("orange")
#
#
# legend_patches = [
#     mpatches.Patch(color="red", label="P9"),
#     mpatches.Patch(color="green", label="P10"),
#     mpatches.Patch(color="orange", label="P11"),
#     mpatches.Patch(color="blue", label="Sisyphus"),
# ]
#
# plt.legend(handles=legend_patches, title="Цвета")
#
# plt.ylabel("Количество пакетов")
# plt.xlabel("Ветка и архитектура")
# plt.title("Количество пакетов в каждой ветке по архитектуре")
# plt.show()


equal_packages_1_2 = second_branch_packages.join(first_branch_packages, on=["name", "arch"], how="inner")

equal_packages_2_3 = third_branch_packages.join(second_branch_packages, on=["name", "arch"], how="inner")

equal_packages_3_4 = fourth_branch_packages.join(third_branch_packages, on=["name", "arch"], how="inner")

#
# print(len(equal_packages_1_2), len(equal_packages_2_3), len(equal_packages_3_4))
# print(len(second_branch_packages))
print(equal_packages_1_2)