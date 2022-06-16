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
    SYNOPSIS

        Load JSON_FILES into Firestore database. You need to be able to
        access the database from the location of your command line environment.
        That is possible by either running in the cloudshell or compute instance
        of the same project where your database is available.

        Files may reside locally or inside the cloud storage.

        To load local files specify absolute or relative path
        to the files 'mypath1/myfile1.json' 'mypath2/myfile2.json'

        To load from google cloud storage specify paths as following:
        'gs://ncbi-research-pmc.appspot.com/dump/13901.json'

        Multiple files/paths are allowed in one run.

        Read an "Additional info" section in README.md file if you want to
        avoid confirming your access to Cloud API (Firestore) each time.

    EXAMPLES

        Loading from cloud storage:

        \b
        $ cloudpmc-proto-firestore-loader load --collection "article_instances" \\
            gs://ncbi-research-pmc.appspot.com/dump/13901.json \\
            gs://ncbi-research-pmc.appspot.com/dump/14901.json \\
            ...

        Loading from local file system:

        \b
        $ cloudpmc-proto-firestore-loader load --collection "article_instances" \\
            dump/13901.json dump/14901.json ...

    """
    fs_loader = FirestoreLoader()
    collection = kwargs.get("collection")

    for json_file in kwargs.get("json_files"):
        json_file_path = AnyPath(json_file)
        docid = kwargs.get("docid", json_file_path.stem) or json_file_path.stem
        logger.info(f"processing file - {json_file_path} with docid={docid}")
        fs_loader.upload(collection, docid, json_file_path)
