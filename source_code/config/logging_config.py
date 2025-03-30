import logging
import warnings
from pathlib import Path

import coloredlogs

# 로그 파일 경로
glob_log_file: Path = None


class CustomWarningHandler(logging.Handler):
    def emit(self, record):
        # 원본 메시지지
        original_message = record.getMessage()

        # 자동수정 경고 메시지 변환
        if "Autocorrecting" in original_message:
            record.msg = (
                "해당 shp 파일에는 잘못된 방향의 링(Ring)을 포함하는 폴리곤이 있습니다. "
                "자동으로 수정하였지만, ogr2ogr 등을 사용하여 올바르게 수정하는 것이 좋습니다."
            )

        # 변환된 메시지 출력
        super().emit(record)


def setup_logger(
    name: str, log_file: Path = None, level=logging.INFO, encoding: str = "utf-8"
) -> logging.Logger:
    """
    지정된 이름, 로그 레벨 및 파일 핸들러를 사용하여 logger를 설정하고 반환한다.

    Args:
        name (str): logger의 이름.
        log_file (Path): 로그를 기록할 파일의 경로. 기본값은 None (콘솔에만 출력).
        level (int): 로그의 최소 수준. 기본값은 logging.INFO.
        encoding (str): 로그 파일을 생성하는 경우 인코딩 방식. 기본값은 'utf-8'.

    Returns:
        logging.Logger: 설정된 logger 인스턴스.
    """
    global glob_log_file

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 핸들러가 이미 존재하는지 확인
    if logger.handlers:
        return logger

    # 콘솔 핸들러를 생성하고 로그 최소 수준을 설정한다.
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # 콘솔 핸들러의 출력 포맷을 설정한다.
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # coloredlogs 설정
    coloredlogs.install(level=level, logger=logger)

    # 파일 핸들러 설정
    if log_file:
        # 로그 파일 경로 설정
        log_file.parent.mkdir(parents=True, exist_ok=True)
        glob_log_file = log_file
        file_handler = logging.FileHandler(str(glob_log_file), encoding=encoding)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # RuntimeWarning 수집 설정
        warnings.simplefilter("always", RuntimeWarning)
        logging.captureWarnings(True)
        for handler in logger.handlers:
            logging.getLogger("py.warnings").addHandler(handler)
    elif glob_log_file:
        file_handler = logging.FileHandler(str(glob_log_file), encoding=encoding)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
