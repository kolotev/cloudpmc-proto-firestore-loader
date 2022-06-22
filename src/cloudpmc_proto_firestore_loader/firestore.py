import copy
import json
import os
from typing import Generator, Optional, Union

from cloudpathlib import AnyPath
from google.cloud import firestore
from google.cloud.firestore_v1.base_document import DocumentSnapshot
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference
from google.cloud.firestore_v1.types.write import WriteResult

from .helpers import decode_b64_fields, deep_truncate, pp
from .logger import logger
from .timing import Timer

# The `project` parameter is optional and represents which project the client
# will act on behalf of. If not supplied, the client falls back to the default
# project inferred from the environment.
#
# ==== Access to emulator
# import os
# from google.auth.credentials import AnonymousCredentials
# from google.cloud.firestore import Client
# os.environ['FIRESTORE_EMULATOR_HOST'] = '127.0.0.1:8772'
# os.environ['no_proxy'] = 'localhost,127.0.0.1'
# credentials = AnonymousCredentials()
# client_db = Client(project="my-project", credentials=credentials)
#
# ==== Access through ENV to ncbi-research-pmc project's Firestore

GA_CREDENTIALS = AnyPath("/").home() / ".service-account.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GA_CREDENTIALS)
os.environ["no_proxy"] = "localhost,127.0.0.1/8"


class FirestoreDB:
    @Timer()
    def __init__(self) -> None:
        try:
            self._db_ = firestore.Client()
        except Exception as e:
            logger.error(str(e))

    @Timer()
    def upload_document(
        self, collection: str, doc_id: str, json_file_path: AnyPath
    ) -> Optional[WriteResult]:
        try:
            with json_file_path.open() as fd:
                doc = json.load(fd)

                # display truncated base64 decoded  document
                doc_decoded = decode_b64_fields(doc)
                doc_for_display = pp.pformat(deep_truncate(copy.deepcopy(doc_decoded)))
                logger.info(f"loading content\n{doc_for_display}")
                logger.info(f"into collection={collection} with doc_id={doc_id}")

                # load the document into database
                return (
                    self._db_.collection(collection).document(doc_id).set(doc_decoded)
                )

        except Exception as e:
            logger.error(str(e))

    @Timer()
    def get_document(self, collection: str, doc_id: str) -> Optional[DocumentSnapshot]:
        doc_ref: DocumentReference = self._db_.collection(collection).document(doc_id)
        doc: DocumentSnapshot = doc_ref.get()
        return doc if doc.exists else None

    @Timer()
    def get_collections(self) -> Generator[CollectionReference, None, None]:
        for c in self._db_.collections():
            yield c

    @Timer()
    def query(
        self,
        collection: str,
        limit: int,
        order_by: str,
        field: str,
        op: str,
        value: Union[str, int, float],
    ) -> Generator[DocumentSnapshot, None, None]:
        query = self._db_.collection(collection).limit(limit)
        query = query.limit(limit)
        query = query.where(field, op, value)
        if order_by:
            query = query.order_by(order_by)

        results = query.stream()

        return results


# TODO: remove following code later.
# create base64 encoded article header
# $ cat < 13901.meta.xml | base64 -w0 > 13901.meta.xml.b64

# create standard compressed and base64 encoded article header
# $ zstd < 13901.meta.xml | base64 -w0 > 13901.meta.xml.zst.b64

# decode
# $ base64 -d < 13901.meta.xml.b64 > 13901.meta.xml

# decode & decompress
# $ base64 -d < 13901.meta.xml.zst.b64 | zstd -d > 13901.meta.xml
