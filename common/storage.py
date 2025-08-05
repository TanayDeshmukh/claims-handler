import os
from abc import ABC, abstractmethod
from anyio import to_thread
from pathlib import Path
from uuid import uuid4
from functools import lru_cache
from dotenv import load_dotenv



load_dotenv()


class StorageBackend(ABC):
    @abstractmethod
    def store(self, file_bytes: bytes, extension: str) -> str:
        pass


class LocalStorage(StorageBackend):
    def __init__(self):
        self.storage_dir: Path = Path(os.getenv("CLAIMS_DATA_STORAGE"))
        self.sub_folder_depth: int = 4

    def file_path(self, claim_id: str) -> Path:
        claim_id = claim_id.lower()
        sub_folders = [claim_id[i] for i in range(self.sub_folder_depth)]
        # generated_path = self.storage_dir / "/".join(sub_folders) / claim_id
        generated_path = self.storage_dir.joinpath(*sub_folders) / claim_id
        return generated_path

    def _write_file(self, file_path: Path, file_bytes: bytes):
        with open(file_path, "wb") as f:
            f.write(file_bytes)

    async def store(self, file_bytes: bytes, extension: str) -> str:
        claim_id = str(uuid4()).lower()
        file_name = f"{claim_id}.{extension}"
        file_path_base = self.file_path(claim_id)

        file_path_base.mkdir(parents=True, exist_ok=True)

        file_path = file_path_base / file_name

        await to_thread.run_sync(self._write_file, file_path, file_bytes)

        return claim_id

@lru_cache
def get_local_storage():
    return LocalStorage()