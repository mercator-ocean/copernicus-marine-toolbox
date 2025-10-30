import os
import pathlib
import shutil
import tempfile
import uuid
from typing import Optional


class TemporaryPathSaver:
    """
    It creates a temporary file (or directory, for Zarr) in the same folder
    as the target path, and renames it to the final destination only if
    the operation completes successfully.
    """

    def __init__(self, output_path: pathlib.Path):
        self.output_path = output_path.resolve()
        self.dir_path = self.output_path.parent
        self.base_name = self.output_path.name
        self.is_zarr = self.output_path.suffix == ".zarr"

        self.temp_path: Optional[pathlib.Path] = None

    def __enter__(self) -> pathlib.Path:
        if self.is_zarr:
            self.temp_path = self.mkstemp_directory(
                prefix=f"{self.base_name}.",
                dir=self.dir_path,
            )
        else:
            fd, temp_filename = tempfile.mkstemp(
                prefix=f"{self.base_name}.",
                dir=self.dir_path,
            )
            os.close(fd)
            self.temp_path = pathlib.Path(temp_filename)
        return self.temp_path

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clean up temporary file/directory on exception
        if exc_type is not None:
            if self.temp_path:
                if self.temp_path.is_dir():
                    shutil.rmtree(self.temp_path, ignore_errors=True)
                elif self.temp_path.exists():
                    self.temp_path.unlink(missing_ok=True)
            return False

        if self.is_zarr:
            if self.output_path.exists():
                shutil.rmtree(self.output_path)
            self.temp_path.rename(self.output_path)
        else:
            shutil.move(str(self.temp_path), str(self.output_path))

        return False

    def mkstemp_directory(
        self, prefix: str, dir: pathlib.Path
    ) -> pathlib.Path:
        """
        Retrying to avoid collision.
        """
        for _ in range(5):
            rand_suffix = uuid.uuid4().hex[:8]
            temp_name = f"{prefix}{rand_suffix}"
            temp_path = dir / temp_name
            try:
                temp_path.mkdir(parents=True, exist_ok=False)
                return temp_path
            except FileExistsError:
                continue
        else:
            raise FileExistsError(
                "Could not create a unique temporary directory after 5 attempts"
            )
