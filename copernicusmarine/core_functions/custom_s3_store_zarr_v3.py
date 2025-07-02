import logging
import time
from typing import AsyncIterator, Iterable, Optional, Union

import botocore.config
import botocore.exceptions
import botocore.session
from zarr.abc.store import ByteRequest, Store
from zarr.core.buffer import Buffer, BufferPrototype, default_buffer_prototype
from zarr.core.common import BytesLike

from copernicusmarine.core_functions.sessions import (
    get_configured_boto3_session,
)

logger = logging.getLogger("copernicusmarine")


class CustomS3StoreZarrV3(Store):
    def __init__(
        self,
        endpoint: str,
        bucket: str,
        root_path: str,
        copernicus_marine_username: Optional[str] = None,
        number_of_retries: int = 9,
        initial_retry_wait_seconds: int = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._root_path = root_path.lstrip("/")
        self._bucket = bucket
        self.client, _ = get_configured_boto3_session(
            endpoint,
            ["GetObject", "HeadObject", "ListObjectsV2"],
            copernicus_marine_username,
        )

        self.number_of_retries = number_of_retries
        self.initial_retry_wait_seconds = initial_retry_wait_seconds

    def __eq__(self, value: object) -> bool:
        """Equality comparison."""
        return (
            isinstance(value, CustomS3StoreZarrV3)
            and self._root_path == value._root_path
            and self._bucket == value._bucket
            and self.client.meta.endpoint_url == value.client.meta.endpoint_url
        )

    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: Union[ByteRequest, None] = None,
    ) -> Union[Buffer, None]:
        def fn():
            full_key = f"{self._root_path}/{key}"
            try:
                resp = self.client.get_object(
                    Bucket=self._bucket, Key=full_key
                )
                res = resp["Body"].read()
                return prototype.buffer.from_bytes(res)
            except botocore.exceptions.ClientError as e:
                https_status_code: int = e.response["ResponseMetadata"][
                    "HTTPStatusCode"
                ]
                if https_status_code == 404 or https_status_code == 403:
                    # means the object does not exist
                    return None
                else:
                    raise e

        return self.with_retries(fn)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, Union[ByteRequest, None]]],
    ) -> list[Union[Buffer, None]]:
        return [
            await self.get(key, prototype, byte_range)
            for key, byte_range in key_ranges
        ]

    async def exists(self, key: str) -> bool:
        return self.get(key, default_buffer_prototype()) is not None

    def supports_writes(self) -> bool:
        return False

    async def set(self, key: str, value: Buffer) -> None:
        pass

    def supports_deletes(self) -> bool:
        return False

    async def delete(self, key: str) -> None:
        pass

    def supports_partial_writes(self) -> bool:
        return False

    async def set_partial_values(
        self, key_start_values: Iterable[tuple[str, int, BytesLike]]
    ) -> None:
        pass

    def supports_listing(self) -> bool:
        """Does the store support listing?"""
        return True

    def keys(self, prefix: str = "") -> list[str]:
        keys = []
        cursor = self._root_path
        while True:
            resp = self.client.list_objects_v2(
                Bucket=self._bucket,
                Prefix=self._root_path + prefix,
                StartAfter=cursor,
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

    async def list(self) -> AsyncIterator[str]:
        for key in self.keys():
            yield key

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        for key in self.keys(prefix):
            yield key

    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        for key in self.keys(prefix):
            yield key

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
