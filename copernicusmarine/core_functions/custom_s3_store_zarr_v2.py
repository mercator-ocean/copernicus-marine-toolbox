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
        self.client, _ = get_configured_boto3_session(
            endpoint,
            ["GetObject", "HeadObject", "ListObjectsV2"],
            copernicus_marine_username,
        )

        self.number_of_retries = number_of_retries
        self.initial_retry_wait_seconds = initial_retry_wait_seconds

    def __getitem__(self, key):
        def fn():
            full_key = f"{self._root_path}/{key}"
            try:
                resp = self.client.get_object(
                    Bucket=self._bucket, Key=full_key
                )
                return resp["Body"].read()
            except botocore.exceptions.ClientError as e:
                raise KeyError(key) from e

        return self.with_retries(fn)

    def __contains__(self, key):
        full_key = f"{self._root_path}/{key}"
        try:
            self.client.head_object(Bucket=self._bucket, Key=full_key)
            return True
        except botocore.exceptions.ClientError as e:
            if "404" in str(e) or "403" in str(e):
                return False
            raise

    def __setitem__(self, key, value, headers=None):
        def fn():
            full_key = f"{self._root_path}/{key}"
            final_headers = headers if headers is not None else {}
            self.client.put_object(
                Bucket=self._bucket, Key=full_key, Body=value, **final_headers
            )

        return self.with_retries(fn)

    def __delitem__(self, key):
        def fn():
            full_key = f"{self._root_path}/{key}"
            self.client.delete_object(Bucket=self._bucket, Key=full_key)

        return self.with_retries(fn)

    # Example of headers: {"ContentType": "application/json", "ContentEncoding": "gzip"}
    def set_item_with_headers(self, key, value, headers):
        return self.__setitem__(key, value, headers)

    def keys(self):
        keys = []
        cursor = self._root_path
        while True:
            resp = self.client.list_objects_v2(
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
            self.client.delete_objects(
                Bucket=self._bucket, Delete={"Objects": objects}
            )
            idx += 1000

    def with_retries(self, fn):
        retry_delay = self.initial_retry_wait_seconds
        for index_try in range(self.number_of_retries):
            try:
                return fn()
            # KeyError is a normal error that we want to propagate
            # (e.g. if we try to get a chunk and it doesn't exist,
            # we want the caller to know this has happened -- and not retry!)
            except KeyError:
                raise
            except Exception as e:
                if index_try == self.number_of_retries - 1:
                    raise e
                logger.debug(f"S3 error: {e}")
                logger.debug(f"Retrying in {retry_delay} s...")
                time.sleep(retry_delay)
                retry_delay *= 2
