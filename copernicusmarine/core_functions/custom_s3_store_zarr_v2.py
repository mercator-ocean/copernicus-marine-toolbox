import logging
import time
from collections.abc import MutableMapping
from typing import Optional

import botocore.config
import botocore.exceptions
import botocore.session

from copernicusmarine.core_functions.sessions import (
    get_configured_boto3_session,
)

logger = logging.getLogger("copernicusmarine")


class CustomS3StoreZarrV2(MutableMapping):
    def __init__(
        self,
        endpoint: str,
        bucket: str,
        root_path: str,
        copernicus_marine_username: Optional[str] = None,
        number_of_retries: int = 9,
        initial_retry_wait_seconds: int = 1,
    ):
        self._root_path = root_path.lstrip("/")
        self._bucket = bucket
        self._endpoint = endpoint
        self._copernicus_marine_username = copernicus_marine_username

        self.number_of_retries = number_of_retries
        self.initial_retry_wait_seconds = initial_retry_wait_seconds

        self._client = None

    def __getstate__(self):
        """
        Ensure boto3 client isn't pickled.
        """
        st = self.__dict__.copy()
        st["_client"] = None
        return st

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._client = None

    def _get_client(self):
        if self._client is None:
            logger.debug("Creating new boto3 client")
            client, _ = get_configured_boto3_session(
                self._endpoint,
                ["GetObject", "HeadObject", "ListObjectsV2"],
                self._copernicus_marine_username,
            )
            self._client = client
        return self._client

    def __getitem__(self, key):
        def fn():
            full_key = f"{self._root_path}/{key}"
            try:
                resp = self._get_client().get_object(
                    Bucket=self._bucket, Key=full_key
                )
                res = resp["Body"].read()
                return res
            except botocore.exceptions.ClientError as e:
                raise KeyError(key) from e

        return self.with_retries(fn)

    def __contains__(self, key):
        full_key = f"{self._root_path}/{key}"

        def fn():
            try:
                self._get_client().head_object(
                    Bucket=self._bucket, Key=full_key
                )
                return True
            except botocore.exceptions.ClientError as e:
                if "404" in str(e) or "403" in str(e):
                    return False
                raise

        return self.with_retries(fn)

    def __setitem__(self, key, value, headers=None):
        def fn():
            full_key = f"{self._root_path}/{key}"
            final_headers = headers if headers is not None else {}
            self._get_client().put_object(
                Bucket=self._bucket, Key=full_key, Body=value, **final_headers
            )

        return self.with_retries(fn)

    def __delitem__(self, key):
        def fn():
            full_key = f"{self._root_path}/{key}"
            self._get_client().delete_object(Bucket=self._bucket, Key=full_key)

        return self.with_retries(fn)

    # Example of headers: {"ContentType": "application/json", "ContentEncoding": "gzip"}
    def set_item_with_headers(self, key, value, headers):
        return self.__setitem__(key, value, headers)

    def keys(self):
        keys = []
        cursor = self._root_path
        while True:
            resp = self._get_client().list_objects_v2(
                Bucket=self._bucket, Prefix=self._root_path, StartAfter=cursor
            )
            entries = resp.get("Contents", [])
            keys += [
                o["Key"].removeprefix(self._root_path).lstrip("/")
                for o in entries
            ]
            if not resp["IsTruncated"]:
                break
            cursor = entries[-1]["Key"]
        return keys

    def __iter__(self):
        keys = self.keys()
        yield from keys

    def __len__(self):
        return len(self.keys())

    def clear(self):
        keys = self.keys()
        idx = 0
        while idx < len(keys):
            some_keys = keys[idx : idx + 1000]
            objects = list(
                map(lambda k: {"Key": f"{self._root_path}/{k}"}, some_keys)
            )
            self._get_client().delete_objects(
                Bucket=self._bucket, Delete={"Objects": objects}
            )
            idx += 1000

    def with_retries(self, fn):
        retry_delay = self.initial_retry_wait_seconds
        for index_try in range(self.number_of_retries):
            try:
                return fn()
            except Exception as e:
                if index_try == self.number_of_retries - 1:
                    raise e
                logger.debug(f"S3 error: {e}")
                logger.debug(f"Retrying in {retry_delay} s...")
                time.sleep(retry_delay)
                retry_delay *= 2
