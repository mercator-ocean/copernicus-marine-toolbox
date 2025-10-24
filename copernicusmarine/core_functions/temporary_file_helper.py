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
        rand_suffix = uuid.uuid4().hex[:8]
        self.suffix = f".{rand_suffix}"
        self.tmp_path: Optional[pathlib.Path] = None
        self._active = False

    def __enter__(self) -> pathlib.Path:
        if self.is_zarr:
            tmp_name = f"{self.base_name}{self.suffix}"
            self.tmp_path = self.dir_path / tmp_name
            self.tmp_path.mkdir(parents=True, exist_ok=False)
        else:
            _, tmp_filename = tempfile.mkstemp(
                prefix=f"{self.base_name}.",
                suffix=self.suffix,
                dir=self.dir_path,
            )
            self.tmp_path = pathlib.Path(tmp_filename)
        self._active = True
        return self.tmp_path

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                if self.is_zarr:
                    if self.output_path.exists():
                        shutil.rmtree(self.output_path)
                    self.tmp_path.rename(self.output_path)
                else:
                    shutil.move(str(self.tmp_path), str(self.output_path))
            else:
                raise exc_val
        except Exception:
            if self.tmp_path:
                if self.tmp_path.is_dir():
                    shutil.rmtree(self.tmp_path, ignore_errors=True)
                elif self.tmp_path.exists():
                    self.tmp_path.unlink(missing_ok=True)
        finally:
            self._active = False
        return False
