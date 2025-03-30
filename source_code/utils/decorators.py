import time
from pathlib import Path

from source_code.config.logging_config import setup_logger

logger = setup_logger(__name__)


def check_new_file(func) -> bool:
    """
    파일 다운로드를 시작한 후, 새로운 파일을 체크하여 다운로드가 완료되었는지 확인하는 데코레이터이다.
    지정된 타임아웃 내에 다운로드가 완료되면 True를 반환하고, 타임아웃이 지나면 False를 반환한다.

    Args:
        func (function): 다운로드를 시작하는 함수

    Returns:
        bool: 다운로드가 성공적으로 완료되었으면 True, 실패하거나 타임아웃이 발생하면 False
    """

    def wrapper(self, **kwargs):
        timeout = kwargs.get("timeout", 60 * 60)  # 기본 타임아웃: 1시간

        # 다운로드 디렉토리의 기존 파일 목록
        initial_files = set(self.download_dir.glob("*"))

        # 다운로드 제한 시간 설정
        end_time = time.time() + timeout

        # 다운로드 시작
        download_clicked = func(self, **kwargs)
        if not download_clicked:
            return False  # 다운로드가 시작되지 않으면 False 반환

        # 다운로드가 완료될 때까지 대기
        while time.time() < end_time:
            # 신규파일
            new_files = set(self.download_dir.glob("*")) - initial_files

            # 다운로드 중인 임시파일
            tmp_files = {
                f for f in new_files if str(f).lower().endswith((".crdownload", ".tmp"))
            }

            # 다운로드 완료된 파일
            downloaded_files: set[Path] = new_files - tmp_files
            if downloaded_files:
                logger.info("파일 다운로드 완료")
                return True
            else:
                time.sleep(10)

        return False

    return wrapper
