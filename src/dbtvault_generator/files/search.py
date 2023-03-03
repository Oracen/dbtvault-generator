from pathlib import Path
from typing import List


def find_files(
    search_dir: Path,
    search_string: str,
    recursive: bool = True,
) -> List[Path]:
    operation = search_dir.rglob if recursive else search_dir.glob
    return list(operation(search_string))
