import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

load_dotenv()
app = typer.Typer(
    name="eokit",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)


def setup_logging(log_level: str, suppress: dict[str, list[str]] | None = None):
    """Configure logging based on level and optionally suppress noisy loggers.

    Args:
        log_level (str): log level, can be info, debug, warning, error
        suppress (dict[str, list[str]] | None, optional): names of the packages to suppress. Defaults to None.
    """
    # Convert string to logging level
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    suppress = suppress or {}
    for log_level, loggers in suppress.items():
        for logger_name in loggers:
            logging.getLogger(logger_name).setLevel(log_level.upper())


@app.callback()
def main(log_level: Annotated[str, typer.Option("--log-level", "-l", help="Set logging level")] = "INFO"):
    setup_logging(
        log_level,
        suppress={
            "error": ["urllib3", "requests", "satpy.readers.core.loading", "pyresample.area_config"],
            "warning": ["satpy", "pyspectral"],
        },
    )


@app.command()
def download(
    sources: list[str],
    start: Annotated[datetime, typer.Option("--start", "-s", help="Start time interval.")],
    end: Annotated[datetime, typer.Option("--end", "-e", help="End time interval.")],
    area_file: Annotated[Path, typer.Option("--area", "-a", help="Path to a GeoJSON file containing the AoI")],
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", "-o", help="Path to where the outputs will be stored"),
    ] = None,
    progress: Annotated[
        str | None,
        typer.Option("--progress", "-p", help="Which progress reporter to use, defaults to none."),
    ] = None,
):
    from satctl.model import SearchParams
    from satctl.progress import create_reporter
    from satctl.sources import create_source, registry

    if "all" in sources:
        sources = registry.list()
    output_dir = output_dir or Path("outputs/downloads")

    search_params = SearchParams(start=start, end=end, area=area_file)
    create_reporter(reporter_name=progress)

    for source_name in sources:
        output_subdir = output_dir / source_name.lower()
        source = create_source(source_name)
        items = source.search(params=search_params)
        source.download(items, output_dir=output_subdir)


@app.command()
def convert(
    sources: list[str],
    area_file: Annotated[Path, typer.Option("--area", "-a", help="Path to a GeoJSON file containing the AoI")],
    source_dir: Annotated[
        Path | None,
        typer.Option("--source-dir", "-s", help="Directory containing raw files"),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", "-o", help="Where to store processed files"),
    ] = None,
    crs: Annotated[str, typer.Option("--crs", help="Coordinate Reference System for the output files")] = "EPSG:4326",
    force_conversion: Annotated[
        bool, typer.Option("--force-conversion", "-f", help="Execute also on already processed files")
    ] = False,
    writer_name: Annotated[
        str, typer.Option("--writer", "-w", help="Which writer to use to save results")
    ] = "geotiff",
    reporter_name: Annotated[
        str | None, typer.Option("--progress", "-p", help="Which progress reporter to use, defaults to none.")
    ] = None,
):
    from satctl.model import ConversionParams
    from satctl.progress import create_reporter
    from satctl.sources import create_source, registry
    from satctl.writers import create_writer

    assert sources, "At least one source is required"
    source_dir = source_dir or Path("outputs/downloads")
    output_dir = output_dir or Path("outputs/processed")

    params = ConversionParams(area=area_file, crs_data=crs)
    writer = create_writer(writer_name=writer_name)
    create_reporter(reporter_name=reporter_name)

    if "all" in sources:
        sources = registry.list()

    for source_name in sources:
        source = create_source(source_name)
        source_subdir = source_dir / source_name.lower()
        output_subdir = output_dir / source_name.lower()

        if source_subdir.exists():
            source.convert(
                params=params,
                source=source_subdir,
                output_dir=output_subdir,
                writer=writer,
                force=force_conversion,
            )
        else:
            typer.echo(f"Warning: No data found for {source_name} in {source_dir}")
