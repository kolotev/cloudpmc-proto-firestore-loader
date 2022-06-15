import json
import timeit

from cloudpathlib import AnyPath
from google.cloud import firestore

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
                logger.info(
                    f"loading content\n{json.dumps(doc, indent=4, sort_keys=True)}"
                )
                logger.info(
                    f"into collection={collection} with docid={docid}"
                )
                self._db_.collection(collection).document(docid).set(doc)
        except Exception as e:
            logger.error(str(e))

        upload_stopped = timeit.default_timer()

        logger.info(f"Completed in {str(upload_stopped - upload_started)} sec.")
