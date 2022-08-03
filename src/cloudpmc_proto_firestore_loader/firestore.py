import json
import os
import re
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from cloudpathlib import AnyPath
from google.cloud import firestore
from google.cloud.firestore_v1.base_document import DocumentSnapshot
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference
from google.cloud.firestore_v1.types.write import WriteResult

from .helpers import (
    decode_b64_fields,
    decode_b64_zcompress_fields,
    simplest_type,
    zdecompress_b64_encode_fields,
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
    # https://github.com/googleapis/python-firestore/blob/main/google/cloud/firestore_v1/base_query.py#L73,L76
    "array_contains",
    "array_contains_any",
    "in",
    "not-in",
]


class _FirestoreDB:
    def __init__(self):
        self._db = None

    @property
    def db(self):
        if self._db is None:
            self._db = firestore.Client()
        return self._db

    @Timer()
    def upload_document(
        self, collection: str, doc_id: str, json_file_path: AnyPath
    ) -> Tuple[Dict[str, Any], Optional[WriteResult]]:
        with json_file_path.open() as fd:
            doc_dict = json.load(fd)
            # decode fields with .b64 suffix in the name of properties
            decode_b64_fields(doc_dict)

            _doc_id = doc_id or doc_dict.get("_id") or json_file_path.stem
            if not _doc_id:
                raise ValueError(
                    f"Document id is required. `_id` is expected in {json_file_path} "
                    "or with --doc-id option in command line."
                )
            elif isinstance(_doc_id, float):
                _doc_id = int(_doc_id)

            _doc_id = str(_doc_id)

            _collection = collection or doc_dict.get("_collection")
            if not _collection:
                raise ValueError(
                    f"Collection name is required. `_collection` is expected in {json_file_path} "
                    "or with --collection option in command line."
                )

            # decode header_xml for article_instances collection
            if _collection == "article_instances" and "header_xml" in doc_dict:
                decode_b64_zcompress_fields(doc_dict, ["header_xml"])
            logger.info(
                f"document with doc_id={_doc_id} is being loaded "
                f"into into collection={_collection}"
            )

            # remove unwanted fields:
            doc_dict.pop("_id")
            doc_dict.pop("_collection")

            # load the document into database
            write_result = self.db.collection(_collection).document(_doc_id).set(doc_dict)
            return doc_dict, write_result

    @Timer()
    def get_document(self, collection: str, doc_id: str) -> Optional[Dict[str, Any]]:
        doc_ref: DocumentReference = self.db.collection(collection).document(doc_id)
        doc: DocumentSnapshot = doc_ref.get()
        doc_dict = None
        if doc.exists:
            doc_dict = doc.to_dict()
            zdecompress_b64_encode_fields(doc_dict, ["header_xml_zstd"])

        return doc_dict

    def get_collections(self) -> Generator[CollectionReference, None, None]:
        for c in self.db.collections():
            yield c

    @Timer()
    def query(
        self, collection: str, limit: int, order_by: str, conditions: List[str]
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        query = self.db.collection(collection).limit(limit)
        query = query.limit(limit)

        for condition in conditions:
            field, op, value = self._parse_condition(condition)
            query = query.where(field, op, value)

        if order_by:
            query = query.order_by(order_by)

        for doc in query.stream():
            doc_dict = doc.to_dict()
            zdecompress_b64_encode_fields(doc_dict, ["header_xml_zstd"])
            yield doc.id, doc_dict

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


db = _FirestoreDB()

__all__ = ["db", "FS_DB_SUPPORTED_OPS"]
