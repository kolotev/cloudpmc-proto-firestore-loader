import copy
from typing import Union

import click
from cloudpathlib import AnyPath

from . import click_types
from .firestore import FirestoreDB
from .helpers import deep_truncate, pp
from .logger import CONFIG, CONFIG_DEBUG, logger
from .timing import Timer

ERROR_NO_DOC = 1
ERROR_QUERY = 2


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

        doc = fdb.get_document(collection, doc_id)
        logger.debug(doc)
        if doc is not None:
            doc_for_display = deep_truncate(copy.deepcopy(doc.to_dict()), 64)
            logger.info("\n{}", pp.pformat(doc_for_display))
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
    for c in fdb.get_collections():
        logger.info(f"\t{c.id} size={len(c.get())}")


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
@click.argument(
    "field",
    nargs=1,
    required=True,
)
@click.argument(
    "op",
    nargs=1,
    required=True,
    type=click.Choice(
        [
            "<",
            "<=",
            "==",
            ">=",
            ">",
            "!=",
            "array-contains",
            "array-contains-any",
            "in",
            "not-in",
        ]
    ),
)
@click.argument("value", nargs=1, required=True, type=click_types.PyEvalType())
@click.pass_context
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
    $ cloudpmc-proto-firestore-loader query  --collection "article-instances"  pmcid '==' PMC13901
    """
    fdb = FirestoreDB()
    collection: str = kwargs.get("collection")
    limit: int = kwargs.get("limit")
    order_by: str = kwargs.get("orderby")
    field: str = kwargs.get("field")
    op: str = kwargs.get("op")
    value: Union[str, int, float] = kwargs.get("value")

    with Timer("query & fetch"):
        found = 0
        try:
            docs = fdb.query(collection, limit, order_by, field, op, value)
            for doc in docs:
                found += 1
                if doc is not None:
                    doc_for_display = deep_truncate(copy.deepcopy(doc.to_dict()), 64)
                    logger.info("\n{}", pp.pformat(doc_for_display))
        except Exception as e:
            logger.error(f"{e} type(e)={type(e)}")
            click_ctx.exit(ERROR_QUERY)

        if found == 0:
            logger.error(f"No documents found in collection={collection}")
        else:
            logger.info(
                f"Found {found} document(s) in collection={collection} with limit={limit}"
            )
