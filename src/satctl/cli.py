from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

import typer
from dotenv import load_dotenv

from satctl.progress.base import ProgressReporter
from satctl.utils import setup_logging

load_dotenv()
app = typer.Typer(
    name="satctl",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)


@dataclass
class CLIContext:
    """Runtime context for CLI commands."""

    progress_reporter: ProgressReporter | None = None


cli_context = CLIContext()


def init_reporter() -> None:
    """Initialize and start the progress reporter.

    Raises:
        ValueError: If no progress reporter is configured in context
    """
    if cli_context.progress_reporter is None:
        raise ValueError(
            "Invalid configuration: progress reporter not found in context "
            "(ensure at least an 'empty' reporter is registered)"
        )
    cli_context.progress_reporter.start()


@app.callback()
def main(
    log_level: Annotated[str, typer.Option("--log-level", "-l", help="Set logging level")] = "INFO",
    progress: Annotated[Literal["empty", "simple", "rich"], typer.Option("--progress", "-p")] = "empty",
):
    """Configure global logging and progress reporting for CLI.

    Args:
        log_level (str): Logging level. Defaults to "INFO".
        progress (Literal["empty", "simple", "rich"]): Progress reporter type. Defaults to "empty".
    """
    from satctl.progress import create_reporter, registry

    reporter_cls = registry.get(progress)
    setup_logging(
        log_level=log_level,
        reporter_cls=reporter_cls,
        suppressions={
            "error": ["urllib3", "requests", "satpy.readers.core.loading", "pyresample.area_config"],
            "warning": ["satpy", "pyspectral", "boto3", "botocore", "s3transfer"],
        },
    )
    cli_context.progress_reporter = create_reporter(reporter_name=progress)


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
    num_workers: Annotated[
        int | None, typer.Option("--num-workers", "-nw", help="Workers count for parallel processing")
    ] = None,
):
    """Download satellite data from specified sources.

    Args:
        sources (list[str]): List of source names or ["all"] for all sources
        start (datetime): Start of time range
        end (datetime): End of time range
        area_file (Path): Path to GeoJSON file defining area of interest
        output_dir (Path | None): Output directory. Defaults to None.
        num_workers (int | None): Number of parallel workers. Defaults to None.
    """
    from satctl.model import SearchParams
    from satctl.sources import create_source, registry

    init_reporter()
    if "all" in sources:
        sources = registry.list()
    output_dir = output_dir or Path("outputs/downloads")

    search_params = SearchParams.from_file(path=area_file, start=start, end=end)
    for source_name in sources:
        output_subdir = output_dir / source_name.lower()
        source = create_source(source_name)
        items = source.search(params=search_params)
        source.download(items, destination=output_subdir, num_workers=num_workers)


@app.command()
def convert(
    sources: list[str],
    area_file: Annotated[
        Path | None, typer.Option("--area", "-a", help="Path to a GeoJSON file containing the AoI")
    ] = None,
    input_dir: Annotated[
        Path | None,
        typer.Option("--input-dir", "-s", help="Directory containing raw files"),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", "-o", help="Where to store processed files"),
    ] = None,
    crs: Annotated[str, typer.Option("--crs", help="Coordinate Reference System for the output files")] = "EPSG:4326",
    datasets: Annotated[
        list[str] | None, typer.Option("--datasets", "-d", help="List of satpy datasets (or composites) to save")
    ] = None,
    resolution: Annotated[
        int | None, typer.Option("--resolution", "-r", help="Custom output resolution for the raw inputs")
    ] = None,
    force_conversion: Annotated[
        bool, typer.Option("--force-conversion", "-f", help="Execute also on already processed files")
    ] = False,
    writer_name: Annotated[
        str, typer.Option("--writer", "-w", help="Which writer to use to save results")
    ] = "geotiff",
    num_workers: Annotated[
        int | None, typer.Option("--num-workers", "-nw", help="Workers count for parallel processing")
    ] = None,
):
    """Convert downloaded satellite data to processed outputs.

    Args:
        sources (list[str]): List of source names or ["all"] for all sources
        area_file (Path | None): Path to GeoJSON file for area of interest. Defaults to None.
        input_dir (Path | None): Directory with raw downloaded files. Defaults to None.
        output_dir (Path | None): Directory for processed outputs. Defaults to None.
        crs (str): Target coordinate reference system. Defaults to "EPSG:4326".
        datasets (list[str] | None): Datasets to process. Defaults to None.
        resolution (int | None): Output resolution. Defaults to None.
        force_conversion (bool): Force reprocessing of existing files. Defaults to False.
        writer_name (str): Writer to use for outputs. Defaults to "geotiff".
        num_workers (int | None): Number of parallel workers. Defaults to None.
    """
    from satctl.model import ConversionParams, Granule
    from satctl.sources import create_source, registry
    from satctl.writers import create_writer

    input_dir = input_dir or Path("outputs/downloads")
    output_dir = output_dir or Path("outputs/processed")
    init_reporter()

    if area_file is not None:
        params = ConversionParams.from_file(
            path=area_file,
            target_crs=crs,
            datasets=datasets,
            resolution=resolution,
        )
    else:
        params = ConversionParams(
            target_crs=crs,
            datasets=datasets,
            resolution=resolution,
        )
    writer = create_writer(writer_name=writer_name)

    if "all" in sources:
        sources = registry.list()

    for source_name in sources:
        source = create_source(source_name)
        source_subdir = input_dir / source_name.lower()
        output_subdir = output_dir / source_name.lower()

        if source_subdir.exists():
            items = [Granule.from_file(f) for f in source_subdir.glob("*") if f.is_dir()]
            source.save(
                items=items,
                params=params,
                destination=output_subdir,
                writer=writer,
                force=force_conversion,
                num_workers=num_workers,
            )
        else:
            typer.echo(f"Warning: No data found for {source_name} in {source_subdir}")


if __name__ == "__main__":
    app()
