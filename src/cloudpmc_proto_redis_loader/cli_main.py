from pathlib import Path
from typing import List

import click
from cloudpathlib import AnyPath

from cloudpmc_proto_firestore_loader.helpers import (
    cli_try_except,
    log_debug_doc_dict,
    save_json_doc_dict,
)
from cloudpmc_proto_firestore_loader.logger import CONFIG, CONFIG_DEBUG, logger
from cloudpmc_proto_firestore_loader.timing import Timer
from cloudpmc_proto_redis_loader import redis

ERROR_NO_DOC = 1
ERROR_QUERY = 2
ERROR_LOAD = 3
# ERROR_LIST_COLLECTIONS = 4
ERROR_GET = 5
ERROR_LOAD_ENCOUNTERED = 6
ERROR_DELETE = 7


@click.group()
@click.option(
    "--debug",
    "-d",
    is_flag=True,
    show_default=True,
    default=False,
    help="Debug this application.",
)
@click.pass_context
def cli_main(click_ctx, *args, debug=None) -> None:
    click_ctx.arg_debug = debug
    if debug:
        logger.configure(**CONFIG_DEBUG)
    else:
        logger.configure(**CONFIG)


@cli_main.command()
@click.option(
    "--collection",
    "-c",
    type=str,
    help="RedisJSON collection name.",
)
@click.option(
    "--doc-id",
    "-i",
    type=str,
    help="Document Id in RedisJSON.",
)
@click.option(
    "--skip-errors",
    "-s",
    is_flag=True,
    show_default=True,
    default=False,
    help="Report and skip individual file loading error.",
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
    Load JSON_FILES into RedisJSON.

    SYNOPSIS

    Load JSON_FILES into RedisJSON database.

    Files may reside locally or in the cloud storage bucket.

    To load local files specify absolute or relative path
    to the files 'my_path1/myfile1.json' 'my_path2/myfile2.json'

    To load from google cloud storage specify paths as following:
    'gs://ncbi-research-pmc.appspot.com/dump/13901.json'

    Multiple files/paths are allowed in one run.

    By default the script picks an id of the document from a "_id" field
    of requested to be loaded json file. If it is not there, the base name
    of the document is used, if you want to force a specific document id
    on the loaded document, you can provide --doc-id option with value. But
    the later one would work only for one document, if multiple documents are
    provided, the same id will be used for all of them.

    Similar appropach is taken for collection, if "_collection" field is
    provided in reqestied to be loaded json document, then it is used by
    default, if it is not there or you want to force a document into specific
    collection you can specify one with --collection COLLECTION_VALUE option.

    EXAMPLES

    Loading from cloud storage:

    \b
    $ redis-loader load --collection "collection_name" \\
        gs://ncbi-research-pmc.appspot.com/dump/13901.json \\
        gs://ncbi-research-pmc.appspot.com/dump/14901.json \\
            ...

    Loading from local file system:

    \b
    $ redis-loader load --collection "collection_name" \\
        dump/13901.json dump/14901.json ...

    """
    collection = kwargs.get("collection")
    skip_errors = kwargs.get("skip_errors")

    errors_encountered = 0
    for json_file in kwargs.get("json_files"):
        try:
            json_file_path = AnyPath(json_file)
            doc_id = kwargs.get("doc_id")
            logger.info(
                f"processing file - {json_file_path}" + f"with doc_id={doc_id}" if doc_id else ""
            )
            doc_dict, _ = redis.db.upload_document(collection, doc_id, json_file_path)
            log_debug_doc_dict(click_ctx, doc_dict)
        except Exception as e:
            errors_encountered += 1
            if skip_errors:
                logger.error(f"{e.__class__.__name__}: {e}")
                continue
            else:
                raise e

    if errors_encountered:
        logger.error(f"Total {errors_encountered} error(s) had been occured.")
        click_ctx.exit(ERROR_LOAD_ENCOUNTERED)


@cli_main.command()
@click.option(
    "--collection",
    "-c",
    type=str,
    help="RedisJSON collection name.",
    required=True,
)
@click.option(
    "--dst",
    "-t",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    help="Destination folder for json files",
    default="/tmp",
    show_default=True,
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
    Get from RedisJSON & store document locally.

    SYNOPSIS

    Get document from RedisJSON and store it locally.

    EXAMPLES

    \b
    $ redis-loader get --collection "collection_name"  13901 14901 ...
    """
    collection = kwargs.get("collection")
    dst: Path = kwargs.get("dst")

    for doc_id in kwargs.get("doc_ids"):
        info = f"retrieving  document from collection={collection} with doc_id={doc_id}"
        logger.info(info)

        doc_dict = redis.db.get_document(collection, doc_id)
        if doc_dict is not None:
            # log_debug_doc_dict(click_ctx, doc_dict)
            save_json_doc_dict(click_ctx, doc_dict, doc_id, dst)
            pass

        else:
            logger.error(
                f"No document with doc_id={doc_id} in collection={collection}, "
                f"check the collection name or doc_ids argument."
            )
            click_ctx.exit(ERROR_NO_DOC)


@cli_main.command()
@click.option(
    "--index",
    "-i",
    type=str,
    help="RedisJSON index name.",
    required=True,
)
@click.option(
    "--limit",
    type=int,
    help="Number of records in result-set.",
    show_default=True,
    default=5,
)
@click.option(
    "--offset",
    type=int,
    help="offset in result-set.",
    show_default=True,
    default=0,
)
@click.option(
    "--dst",
    "-t",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    help="Destination folder for json files",
    default="/tmp",
    show_default=True,
)
@click.argument("conditions", nargs=-1, required=True)
@click.pass_context
@cli_try_except(ERROR_QUERY)
# @docstring_with_params(ops=firestore.FS_DB_SUPPORTED_OPS)
def query(click_ctx, *args, **kwargs) -> None:
    """
    Find document(s) in RedisJSON.

    SYNOPSIS

    To find document(s) in RedisJSON you need to have RediSearch module
    available in your Redis instance with indecies set.
    When you use this command you need to specify certain values as they are
    defined in python.

    EXAMPLES

    \b
    $ redis-loader query --index "idx:ai" '@pmcid:{PMC13901}' '@version:[1 inf]'
    $ redis-loader query --index "idx:ai" '@aiid:[13901 13901]'
    $ redis-loader query --index "idx:ai" '@pmid:[11250747 11250747]'
    $ redis-loader query --index "idx:ai" '@doi:{10\\.1186\\/bcr272}'
    $ redis-loader query --index "idx:ai" \\
        '(@ivip:{1465\\-5411\\/3\\/1\\/61})|(@ivip:{1465\\-542X\\/3\\/1\\/61})'
    $ redis-loader query --index "idx:ai" '@ivip:{1465\\-5411\\/3\\/1\\/61}'
    $ redis-loader query --index "idx:ai" '(@pmcid:{PMC13901})|(@pmcid:{PMC14901})'
    $ redis-loader -d query --index "idx:ai" '@is_oa:{true}'
    $ redis-loader -d query --index "idx:ai" '*' # all records from index with limit & offset

    $ redis-loader -d query --index "idx:jl" '@domain_id:{2492}'
    $ redis-loader -d query --index "idx:jl" '@jtitle:(ANN MED)' # search for docs with ANN and MED
    $ redis-loader -d query --index "idx:jl" '%ANG%' # fuzzy search with one step LD distance
    $ redis-loader -d query --index "idx:jl" '%%AHH%%'  # fuzzy search with two step LD distance

    Notes:

    Check the syntax of the queries you can make. Mapping of SQL to Redis commands,
    could help you to prepare your queries
    https://redis.io/docs/stack/search/reference/query_syntax/#mapping-common-sql-predicates-to-redisearch

    on Fuzzy search queries conslt with
    # consult with https://redis.io/docs/stack/search/reference/query_syntax/#fuzzy-matching


    EXPECTED SCHEMAS

    See schema at
    https://bitbucket.ncbi.nlm.nih.gov/projects/PMC/repos/pmc-cloud-prototype/browse/redis_indices.txt

    """
    index: str = kwargs["index"]
    limit: int = kwargs["limit"]
    offset: int = kwargs["offset"]
    conditions: List[str] = list(kwargs["conditions"])
    dst: Path = kwargs["dst"]

    with Timer("query()"):
        found = 0
        for doc_id, doc_dict in redis.db.query(index, limit, offset, conditions):
            found += 1
            # log_debug_doc_dict(click_ctx, doc_dict)
            save_json_doc_dict(click_ctx, doc_dict, doc_id, dst)

        logger.info(
            f"Found {found} document(s) in index={index} with limit={limit} offset={offset}"
        )

@cli_main.command()
@click.option(
    "--index",
    "-i",
    type=str,
    help="RedisJSON index name.",
    required=True,
)
@click.option(
    "--limit",
    type=int,
    help="Number of records in result-set.",
    show_default=True,
    default=5,
)
@click.option(
    "--offset",
    type=int,
    help="offset in result-set.",
    show_default=True,
    default=0,
)
@click.option(
    "--dst",
    "-t",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    help="Destination folder for json files",
    default="/tmp",
    show_default=True,
)
@click.argument("conditions", nargs=-1, required=True)
@click.pass_context
@cli_try_except(ERROR_QUERY)
# @docstring_with_params(ops=firestore.FS_DB_SUPPORTED_OPS)
def mquery(click_ctx, *args, **kwargs) -> None:
    """
    Find document(s) in RedisJSON based on each condition individually
    instead of using logical AND for all queries in command line.

    SYNOPSIS

    To find document(s) in RedisJSON you need to have RediSearch module
    available in your Redis instance with indecies set.
    When you use this command you need to specify certain values as they are
    defined in python.

    EXAMPLES

    \b
    $ redis-loader mquery --index "idx:ai" '@pmcid:{PMC13901}' '@pmcid:{PMC13902}' ...

    Notes:

    Check the syntax of the queries you can make. Mapping of SQL to Redis commands,
    could help you to prepare your queries
    https://redis.io/docs/stack/search/reference/query_syntax/#mapping-common-sql-predicates-to-redisearch

    on Fuzzy search queries conslt with
    # consult with https://redis.io/docs/stack/search/reference/query_syntax/#fuzzy-matching


    EXPECTED SCHEMAS

    See schema at
    https://bitbucket.ncbi.nlm.nih.gov/projects/PMC/repos/pmc-cloud-prototype/browse/redis_indices.txt

    """
    index: str = kwargs["index"]
    limit: int = kwargs["limit"]
    offset: int = kwargs["offset"]
    conditions: List[str] = list(kwargs["conditions"])
    dst: Path = kwargs["dst"]

    with Timer("mquery()"):
        found = 0
        for condition in conditions:
            for doc_id, doc_dict in redis.db.query(index, limit, offset, [condition]):
                found += 1
                # log_debug_doc_dict(click_ctx, doc_dict)
                save_json_doc_dict(click_ctx, doc_dict, doc_id, dst)

        logger.info(
            f"Found {found} document(s) in index={index} with limit={limit} offset={offset}"
        )

@cli_main.command()
@click.option(
    "--collection",
    "-c",
    type=str,
    help="Firestore collection name.",
    required=True,
)
@click.option(
    "--skip-errors",
    "-s",
    is_flag=True,
    show_default=True,
    default=False,
    help="Report and skip individual deletion error.",
)
@click.argument("doc_ids", nargs=-1, required=True)
@click.pass_context
@cli_try_except(ERROR_DELETE)
def delete(click_ctx, *args, **kwargs) -> None:
    """
    delete document(s)

    SYNOPSIS

    Delete document(s) in RedisJSON by the collection & document id
    or all using "*" argument as document id.

    EXAMPLES

    Delete selected documents in a given collection:
    \b
    $ redis-loader delete --collection "collection_name" doc_id1 doc_id2 ...


    Delete all documents in a given collection:
    \b
    $ redis-loader delete --collection "collection_name" "*"


    NOTES

    To delete all documents from a collection you should specify "*" as
    `doc_id` argument. Use quotes to avoid shell expansion.

    """
    errors_encountered = 0

    collection: str = kwargs["collection"]
    doc_ids = kwargs.get("doc_ids")
    skip_errors = kwargs.get("skip_errors")

    with Timer("delete"):
        for doc_id in doc_ids:
            if doc_id == "*":
                redis.db.delete_all_docs(collection)
            else:
                try:
                    redis.db.delete_doc(collection, doc_id)
                except Exception as e:
                    errors_encountered += 1
                    if skip_errors:
                        logger.error(f"{e.__class__.__name__}: {e}")
                        continue
                    else:
                        raise e

    if errors_encountered:
        logger.error(f"Total {errors_encountered} error(s) had been occured.")
        click_ctx.exit(ERROR_LOAD_ENCOUNTERED)
