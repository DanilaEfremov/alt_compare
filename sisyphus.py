#! /usr/bin/env -S uv run --script  # noqa: D100
import json

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "click==8.2.1",
#     "packaging>=25.0",
#     "polars>=1.32.3",
#     "requests>=2.32.5",
# ]
# ///
from pathlib import Path
from timeit import default_timer as timer

import click
import polars as pl

import compare_module as cmp


# ########################################################################
@click.command()
@click.argument("branch1", type=click.STRING)
@click.argument("branch2", type=click.STRING)
@click.option(
    "--arch", "-a",
    default="all",
    help="Only one architecture. Example: --arch aarch64"
)
@click.option(
    "--packages", "-p",
    default="",
    help="Needed packages names. --packages perl-Authen-Smb-debuginfo, swapd-debuginfo"
)
@click.option(
    "--file",
    default="",
    help="File with packages names. --file names.txt"
)
@click.option(
    "--force", "-f",
    help="Force overwrite of existing branches data.",
    is_flag=True,
    default=False,
)
@click.option(
    "--comp", "-c",
    type=click.Choice(["lt", "gt", "eq", "ge", "le", "ne"]),
    help="How to compare versions of the same packages in different branches. Example: --comp gt",  # noqa: E501
    default="gt",
)
def main(branch1: str, branch2: str, force: bool, arch: str, comp: str, packages: str, file: str) -> None: # noqa: FBT001
    """Compare packages in two different Alt Linux branches.

    BRANCH1, BRANCH2 - names of the branches.
    Example:  sisyphus.py sisyphus p11
    """
    # Находим нужные для вычислений данные
    start_time = timer()
    json2: Path|None = None
    if force:
        click.echo("Forcing overwrite of existing branches data.")
        if cmp.is_branch_cache_exists(branch1):
            cmp.delete_file(cmp.get_branch_file_name(branch1))
        if cmp.is_branch_cache_exists(branch2):
            cmp.delete_file(cmp.get_branch_file_name(branch2))
    json1 = cmp.get_branch_json(branch1, arch)

    if json1:
        click.echo(f"JSON file for branch name {branch1} is {json1}.")
        json2 = cmp.get_branch_json(branch2, arch)
        if json2:
            click.echo(f"JSON file for branch name {branch2} is {json2}.")
        else:
            click.echo(f"Don't know how to get JSON file for branch name {branch2}.")
            return
    else:
        click.echo(f"Don't know how to get JSON file for branch name {branch1}.")
        return

    # Делаем вычисления
    start_calculate_time = timer()
    col = "packages"
    first_branch_packages = pl.read_json(json1).select(col).explode(col).unnest(col)
    second_branch_packages = pl.read_json(json2).select(col).explode(col).unnest(col)


    file_packages = []
    if file != "":
        file_path = Path(file)
        if not file_path.exists():
            click.echo(f"File {file_path} does not exist.")
            exit(1)
        with file_path.open("r") as f:
            content = f.read().replace("\n",",").replace(",,", ",").replace(" ", "")
            file_packages = content.split(",")

    commandline_packages = []
    if packages != "":
        commandline_packages = packages.split(",")
        commandline_packages = list(map(lambda x: x.strip(), commandline_packages))

    names_lst = list(set(file_packages).union(commandline_packages))
    if len(names_lst) > 0:
        first_branch_packages = first_branch_packages.filter(pl.col("name").is_in(names_lst))
        second_branch_packages = second_branch_packages.filter(pl.col("name").is_in(names_lst))

    architectures = (
        second_branch_packages["arch"]
        .unique()
        .append(first_branch_packages["arch"].unique())
        .unique()
    )

    json_data = {}
    for sys_arch in list(architectures):
        first_by_arch = first_branch_packages.filter(
            first_branch_packages["arch"] == sys_arch
        )  # noqa: E501
        second_by_arch = second_branch_packages.filter(
            second_branch_packages["arch"] == sys_arch
        )  # noqa: E501

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
                    lambda s: cmp.compare(
                        s["first_branch_version"], s["second_branch_version"]
                    ),
                    return_dtype=pl.String,
                )
                .alias("compare")
            )
            .filter(cmp.get_filter_expression(comp))
            .select(pl.all().exclude("compare"))
        )

        json_data[sys_arch] = {
            "second_only_count": second_only_packages["name"].count(),
            "second_only_packages": second_only_packages.to_dicts(),
            "first_only_count": first_only_packages["name"].count(),
            "first_only_packages": first_only_packages.to_dicts(),
            "newest_in_first_count": equal_packages["name"].count(),
            "newest_in_first": equal_packages.to_dicts(),
        }

    try:
        output_file_name = "output.json"
        output_file = Path(output_file_name)
        with output_file.open("w+", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
            f.flush()
        end_time = timer()
        click.echo(
            f"Result downloaded in {output_file_name}, took {end_time - start_time} seconds."
        )  # noqa: E501
    except Exception:
        click.echo("Failed to write calculated data.")




if __name__ == "__main__":
    main()
