from .logger import logger
from .loader import FirestoreLoader
from cloudpathlib import AnyPath

import click


@click.group()
@click.pass_context
def cli_main(click_ctx, *args, **kwargs) -> None:
    pass


@cli_main.command()
@click.option(
    "--collection",
    type=str,
    help="Collection name of the Firestore collection to load document to.",
    required=True,
)
@click.option(
    "--docid",
    type=str,
    help="Document Id in Firestore collection, if not provided then the file name would be used.",
    required=False,
)
@click.argument(
    "json_files",
    nargs=-1,
    # FIXME: remove next line or provide a custom time,
    # FIXME: which can handle local or cloud paths
    # type=click.Path(exists=True, readable=True, resolve_path=True, allow_dash=True),
    required=True,
)
@click.pass_context
def load(click_ctx, *args, **kwargs) -> None:
    """
    Load JSON_FILES into Firestore database.
    Files may reside locally or in the cloud storage.

    To load local files specify absolute or relative path
    to the files 'mypath1/myfile1.json' 'mypath2/myfile2.json' ...

    To load from google cloud storage specify path as following ...

    TODO: provide an example of cloud storage path

    """
    fs_loader = FirestoreLoader()
    collection = kwargs.get("collection")

    for json_file in kwargs.get("json_files"):
        json_file_path = AnyPath(json_file)
        logger.info(f"processing file - {json_file_path}")
        docid = kwargs.get("docid", json_file_path.stem)
        fs_loader.upload(collection, docid, json_file_path)
