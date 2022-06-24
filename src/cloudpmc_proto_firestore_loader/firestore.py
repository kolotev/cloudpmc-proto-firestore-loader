import copy
import json
import os
import re
from typing import Generator, List, Optional, Tuple, Union

from cloudpathlib import AnyPath
from google.cloud import firestore
from google.cloud.firestore_v1.base_document import DocumentSnapshot
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference
from google.cloud.firestore_v1.types.write import WriteResult

from .helpers import (
    decode_b64_compress_fields,
    decode_b64_fields,
    deep_truncate,
    pprinter,
    simplest_type,
)
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

# Firestore supported conditional operators to query
FS_DB_SUPPORTED_OPS = [
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
        with json_file_path.open() as fd:
            doc = json.load(fd)

            # decode fields with .b64 suffix in the name of properties
            decode_b64_fields(doc)
            # decode header_xml for article_instances collection
            if collection == "article_instances" and doc.get("header_xml"):
                decode_b64_compress_fields(doc, ["header_xml"])
            doc_display = pprinter.pformat(deep_truncate(copy.deepcopy(doc)))
            logger.info(f"loading content\n{doc_display}")
            logger.info(f"into collection={collection} with doc_id={doc_id}")

            # load the document into database
            return self._db_.collection(collection).document(doc_id).set(doc)

    @Timer()
    def get_document(self, collection: str, doc_id: str) -> Optional[Dict[str, Any]]:
        doc_ref: DocumentReference = self._db_.collection(collection).document(doc_id)
        doc: DocumentSnapshot = doc_ref.get()
        doc_dict = None
        if doc.exists:
            doc_dict = doc.to_dict()
            if collection == "article_instances":
                zdecompress_b64_encode_fields(doc_dict, ["header_xml_zstd"])

        return doc_dict

    @Timer()
    def get_collections(self) -> Generator[CollectionReference, None, None]:
        for c in self._db_.collections():
            yield c

    @Timer()
    def query(
        self, collection: str, limit: int, order_by: str, conditions: List[str]
    ) -> Generator[DocumentSnapshot, None, None]:
        query = self._db_.collection(collection).limit(limit)
        query = query.limit(limit)
        for condition in conditions:
            field, op, value = self._parse_condition(condition)
            query = query.where(field, op, value)
        if order_by:
            query = query.order_by(order_by)

        return query.stream()

    @staticmethod
    def _parse_condition(condition: str) -> Tuple[str, str, Union[str, int, float]]:
        re_pattern = r"^(.*?)(" f"{'|'.join(FS_DB_SUPPORTED_OPS)}" r")(.*?)$"
        re_search = re.search(re_pattern, condition)
        if re_search:
            field, op, value = re_search.groups()
        else:
            raise ValueError(f"condition `{condition}` is not valid.")

        if field == "":
            raise ValueError(f"field can not be empty in condition `{condition}`.")
        if op not in FS_DB_SUPPORTED_OPS:
            raise ValueError(f"unknown operator {op} in condition `{condition}`.")
        if value == "":
            raise ValueError(f"value can not be empty in condition `{condition}`.")

        value = value.strip()
        value = simplest_type(value)
        field = field.strip()
        return (field, op, value)


# TODO: remove following code later.
# create base64 encoded article header
# $ cat < 13901.meta.xml | base64 -w0 > 13901.meta.xml.b64

# create standard compressed and base64 encoded article header
# $ zstd < 13901.meta.xml | base64 -w0 > 13901.meta.xml.zst.b64

# decode
# $ base64 -d < 13901.meta.xml.b64 > 13901.meta.xml

# decode & decompress
# $ base64 -d < 13901.meta.xml.zst.b64 | zstd -d > 13901.meta.xml
