import inspect
from pathlib import Path
from typing import Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent.parent

COLLECTED_DIR = ROOT_DIR / "자동화결과" / "01_수집"
REFINED_DIR = ROOT_DIR / "자동화결과" / "02_정제"
GEOCODING_DIR = ROOT_DIR / "자동화결과" / "03_지오코딩"
OUTPUT_DIR = ROOT_DIR / "자동화결과" / "04_최종SHP"

JUSO_DIR = ROOT_DIR / "source_code" / "boundary" / "juso"
CENSUS_DIR = ROOT_DIR / "source_code" / "boundary" / "census"


def get_directories() -> Tuple[Path, Path, Path, Path, Path, Path]:
    """
    단계별 디렉토리 경로를 반환하는 함수.

    Returns:
        dict: 각 디렉토리 경로를 포함한 튜플 반환
    """
    stack = inspect.stack()
    module_name = Path(stack[1].filename).stem.upper()  # 함수를 호출한 모듈의 이름름

    return (
        COLLECTED_DIR / module_name,
        REFINED_DIR / module_name,
        GEOCODING_DIR / module_name,
        OUTPUT_DIR / module_name,
    )
