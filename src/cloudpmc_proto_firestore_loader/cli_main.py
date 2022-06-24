import copy
from typing import List

import click
from cloudpathlib import AnyPath

from .firestore import FS_DB_SUPPORTED_OPS, FirestoreDB
from .helpers import cli_try_except, docstring_with_params, log_debug_doc_dict
from .logger import CONFIG, CONFIG_DEBUG, logger
from .timing import Timer

ERROR_NO_DOC = 1
ERROR_QUERY = 2
ERROR_LOAD = 3
ERROR_LIST_COLLECTIONS = 4
ERROR_GET = 5


@click.group()
@click.option(
    "--debug",
    is_flag=True,
    show_default=True,
    default=False,
    help="Debug this application.",
)
@click.pass_context
def cli_main(click_ctx, *args, debug=None) -> None:
    click_ctx._debug = debug
    if debug:
        logger.configure(**CONFIG_DEBUG)
    else:
        logger.configure(**CONFIG)


@cli_main.command()
@click.option(
    "--collection",
    type=str,
    help="Firestore collection name.",
    required=True,
)
@click.option(
    "--doc_id",
    type=str,
    help="Document Id in Firestore collection. Base file name is used if not provided the .",
    required=False,
)
@click.argument(
    "json_files",
    nargs=-1,
    required=True,
)
@click.pass_context
@cli_try_except(ERROR_LOAD)
def load(click_ctx, *args, **kwargs) -> None:
    """
    load JSON_FILES into Firestore database.

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
        fdb = FirestoreDB()
        collection = kwargs.get("collection")

        for json_file in kwargs.get("json_files"):
            json_file_path = AnyPath(json_file)
            doc_id = kwargs.get("doc_id", json_file_path.stem) or json_file_path.stem
            logger.info(f"processing file - {json_file_path} with doc_id={doc_id}")
            fdb.upload_document(collection, doc_id, json_file_path)


@cli_main.command()
@click.option(
    "--collection",
    type=str,
    help="Firestore collection name.",
    required=True,
)
@click.argument(
    "doc_ids",
    nargs=-1,
    required=True,
)
@click.pass_context
@cli_try_except(ERROR_GET)
def get(click_ctx, *args, **kwargs) -> None:
    """
    get document from Firestore collection.

    SYNOPSIS

    Get document from Firestore collection.

    EXAMPLES

    \b
    $ cloudpmc-proto-firestore-loader get --collection "article-instances"  13901 14901 ...
    """
    fdb = FirestoreDB()
    collection = kwargs.get("collection")
    for doc_id in kwargs.get("doc_ids"):
        info = f"retrieving  document from collection={collection} with doc_id={doc_id}"
        logger.info(info)

            log_debug_doc_dict(click_ctx, doc_dict)
            json_file = f"{doc_id}.json"
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(doc_dict, f, ensure_ascii=False, indent=4, sort_keys=True)

            logger.info(f"document with doc_id={doc_id} was written into {json_file} file.")

        else:
            logger.error(
                f"No document with doc_id={doc_id} in collection={collection}, "
                f"check the collection name or doc_ids argument."
            )
            click_ctx.exit(ERROR_NO_DOC)


@cli_main.command()
@click.pass_context
def list_collections(click_ctx, *args, **kwargs) -> None:
    """
    get list of top-level collections from Firestore database.

    SYNOPSIS

    Get list of top-level collections from Firestore database.

    WARNING

    It is not known if it causing the read operation on all documents
    of the collection yet.

    EXAMPLES

    \b
    $ cloudpmc-proto-firestore-loader list-collections
    """
    fdb = FirestoreDB()

    logger.info("List of available collections:")
    with Timer("list collections"):
        for c in fdb.get_collections():
            logger.info(f"\t{c.id}")
            # if size is needed the following addition could be made:
            # size={len(c.get())}, but it is causing the # of reads equal to # of docs.
    try:
        c
    except NameError:
        logger.warning("\tNo collections found")


@cli_main.command()
@click.option(
    "--collection",
    type=str,
    help="Firestore collection name.",
    required=True,
)
@click.option(
    "--limit",
    type=int,
    help="Limit number of records in result-set.",
    show_default=True,
    default=5,
)
@click.option(
    "--orderby",
    type=str,
    help=(
        "By default, a query retrieves all documents that satisfy the query "
        "in ascending order by document ID."
        "You can specify the sort order for your data using this option."
    ),
)
@click.argument("conditions", nargs=-1, required=True)
@click.pass_context
@cli_try_except(ERROR_QUERY)
@docstring_with_params(ops=FS_DB_SUPPORTED_OPS)
def query(click_ctx, *args, **kwargs) -> None:
    """
    find document(s) in Firestore collection.

    SYNOPSIS

    Find document(s) in Firestore collection. When you use this command you
    need to specify certain values as they are defined in python.
    For example, when you need to use a boolean value
    use True or False values.

    EXAMPLES

    \b
    $ cloudpmc-proto-firestore-loader query --collection "article_instances" \\
        'pmcid == PMC13901' 'is_oa!=False' ...

    Notes:

    \b
    CONDITION consists of 3 components "FIELD OP VALUE":
        FIELD - field name
        OP - operator supported by Firestore DB.
        VALUE -  value.

    They can be separated by spacing and some may require wrapping into quotes
    to avoid shell redirects.

    List of supported operators: {ops}
    """
    fdb = FirestoreDB()
    collection: str = kwargs["collection"]
    limit: int = kwargs["limit"]
    order_by: str = kwargs["orderby"]
    conditions: List[str] = kwargs["conditions"]

    with Timer("query() & fetch"):
        try:
            found = 0
            for doc in fdb.query(collection, limit, order_by, conditions):
                if doc is not None:
                    found += 1
            log_debug_doc_dict(click_ctx, doc_dict)
            json_file = f"{doc_id}.json"
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(doc_dict, f, ensure_ascii=False, indent=4, sort_keys=True)
            logger.info(f"document with doc_id={doc_id} was written into {json_file} file.")
        logger.info(f"Found {found} document(s) in collection={collection} with limit={limit}")

@cli_try_except(ERROR_LIST_COLLECTIONS)
def list_collections(click_ctx, *args, **kwargs) -> None:

        logger.info(f"Found {found} document(s) in collection={collection} with limit={limit}")
