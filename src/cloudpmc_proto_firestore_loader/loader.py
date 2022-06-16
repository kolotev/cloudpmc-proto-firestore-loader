import copy
import json
import pprint
import timeit

from cloudpathlib import AnyPath
from google.cloud import firestore

from .helpers import decode_b64_fields, deep_truncate
from .logger import logger

# The `project` parameter is optional and represents which project the client
# will act on behalf of. If not supplied, the client falls back to the default
# project inferred from the environment.
#
# project_kwargs = {"project": "my-project-id"}
# db = firestore.Client(**project_kwargs)


class FirestoreLoader:
    def __init__(self) -> None:
        try:
            self._db_ = firestore.Client()
        except Exception as e:
            logger.error(str(e))

    def upload(self, collection: str, docid: str, json_file_path: AnyPath):
        upload_started = timeit.default_timer()

        try:
            with json_file_path.open() as fd:
                doc = json.load(fd)

                # display truncated base64 decoded  document
                pp = pprint.PrettyPrinter(indent=4)
                doc_decoded = decode_b64_fields(doc)
                doc_for_display = pp.pformat(deep_truncate(copy.deepcopy(doc_decoded)))
                logger.info(f"loading content\n{doc_for_display}")
                logger.info(f"into collection={collection} with docid={docid}")

                # load the document into database
                self._db_.collection(collection).document(docid).set(doc_decoded)

        except Exception as e:
            logger.error(str(e))

        upload_stopped = timeit.default_timer()

        logger.info(f"Completed in {str(upload_stopped - upload_started)} sec.")


# TODO: remove following code later.
# create base64 encoded article header
# $ cat < 13901.meta.xml | base64 -w0 > 13901.meta.xml.b64

# create standard compressed and base64 encoded article header
# $ zstd < 13901.meta.xml | base64 -w0 > 13901.meta.xml.zst.b64

# decode
# $ base64 -d < 13901.meta.xml.b64 > 13901.meta.xml

# decode & decompress
# $ base64 -d < 13901.meta.xml.zst.b64 | zstd -d > 13901.meta.xml
