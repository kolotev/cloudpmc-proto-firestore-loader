import base64
import json
import os
from typing import Any, Dict, Generator, List, Optional, Tuple

import redis
from cloudpathlib import AnyPath
from redis.commands.search.query import Query

from cloudpmc_proto_firestore_loader.helpers import (
    b64_decode_zcompress_fields,
    b64_decode_zdecompress_fields,
    chunks,
    decode_b64_fields,
)
from cloudpmc_proto_firestore_loader.logger import logger
from cloudpmc_proto_firestore_loader.timing import Timer

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", str(6370))
REDIS_USER = os.environ.get("REDIS_USER", None)
REDIS_PASS = os.environ.get("REDIS_PASS", None)


class _RedisJsonDB:
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, username=REDIS_USER, password=REDIS_PASS):
        self._db = None
        self._host = host
        self._port = int(port)
        self._user = username
        self._passwd = password

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def user(self):
        return self._user

    @property
    def passwd(self):
        return self._passwd

    @property
    def db(self):
        if self._db is None:
            try:
                self._db = redis.Redis(
                    host=self.host, port=self.port, username=self.user, password=self.passwd
                )  # decode_responses=True
                self._db.ping()
            except Exception:
                logger.warning(
                    "Check environment variables: "
                    "REDIS_HOST, REDIS_PORT, REDIS_USER, REDIS_PASS, "
                    "are they set correctly. "
                    f"Or verify what is being passed to {self.__class__.__name__}() "
                    "to create instance of that class."
                )

        return self._db

    @Timer()
    def upload_document(
        self, collection: str, doc_id: str, json_file_path: AnyPath
    ) -> Tuple[Dict[str, Any], bool]:
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
                pass
                b64_decode_zcompress_fields(doc_dict, ["header_xml"])
                if "header_xml_zstd" in doc_dict:
                    doc_dict["header_xml_zstd"] = base64.b64encode(
                        doc_dict["header_xml_zstd"]
                    ).decode("ascii")
            logger.info(
                f"document with doc_id={_doc_id} is being loaded "
                f"into into collection={_collection}"
            )

            # remove unwanted fields:
            # doc_dict.pop("_id", None)
            # doc_dict.pop("_collection", None)

            write_result = self.db.json().set(f"{_collection}:{_doc_id}", ".", doc_dict)
            return doc_dict, write_result

    @Timer()
    def get_document(self, collection: str, doc_id: str) -> Optional[Dict[str, Any]]:
        doc_dict = self.db.json().get(f"{collection}:{doc_id}")
        b64_decode_zdecompress_fields(doc_dict, ["header_xml_zstd"])

        return doc_dict

    def delete_doc(self, collection: str, doc_id: str) -> bool:
        self.db.json().delete(f"{collection}:{doc_id}")
        logger.info(f"{doc_id} was requested to be deleted")

    def delete_all_docs(self, collection: str, batch_size: int = 100) -> int:
        deleted = 0

        for keys_chunk in chunks(self.db.scan_iter(f"{collection}:*"), batch_size):
            keys_list = list(keys_chunk)
            self.db.delete(*keys_list)
            deleted += len(keys_list)

        if deleted == 1:
            logger.info(f"{deleted} document was deleted.")
        elif deleted > 1:
            logger.info(f"{deleted} documents were deleted.")
        else:
            logger.warning("No documents were deleted, check if your collection has any.")

        return deleted

    def query(
        self, index: str, limit: int, offset: int, conditions: List[str]
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        query = Query(" ".join(conditions))
        if limit is not None and offset is not None:
            query = query.paging(offset, limit)

        for doc in self.db.ft(index).search(query).docs:
            doc_dict = json.loads(doc.json)
            b64_decode_zdecompress_fields(doc_dict, ["header_xml_zstd"])
            yield doc.id.split(":", 1)[1], doc_dict


db = _RedisJsonDB()

__all__ = ["db"]
