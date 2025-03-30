# 팜맵(gm_zlnu_famp_inf)

from datetime import datetime

from source_code.config import paths
from source_code.config.logging_config import setup_logger

# 디렉토리 경로 설정
collected_dir, refined_dir, geocoding_dir, output_dir = paths.get_directories()


# 로그 설정
now = datetime.now()
log_file = now.strftime("%Y-%m-%d_%H%M%S") + ".log"
logger = setup_logger(__name__.split(".")[-1], log_file=output_dir / log_file)

from pathlib import Path

import source_code.utils.file_util as file_utils
from source_code.processors.collection_processor.selenium_scraper import SeleniumScraper


def collect():
    # 셀레니움 스크레이퍼 인스턴스 생성
    collector = SeleniumScraper(collected_dir)

    # 수집 목록
    urls = (
        "https://www.data.go.kr/data/15104483/fileData.do",  # 경기도
        "https://www.data.go.kr/data/15104487/fileData.do",  # 전라남도
        "https://www.data.go.kr/data/15104488/fileData.do",  # 경상북도
        "https://www.data.go.kr/data/15104489/fileData.do",  # 경상남도
        "https://www.data.go.kr/data/15104491/fileData.do",  # 제주특별자치도
        "https://www.data.go.kr/data/15104484/fileData.do",  # 충청북도
        "https://www.data.go.kr/data/15104485/fileData.do",  # 충청남도
        "https://www.data.go.kr/data/15104486/fileData.do",  # 전북특별자치도
        "https://www.data.go.kr/data/15104490/fileData.do",  # 강원특별자치도
        "https://www.data.go.kr/data/15104481/fileData.do",  # 서울_세종_6대광역시
    )

    # 수집 시작
    for url in urls:
        # 페이지 접속
        connected = collector.fetch_page(url)
        if not connected:
            raise Exception("페이지 접속 실패")

        # 파일 수집
        download = collector.file_download(
            xpath='//a[contains(@title, "다운로드") and contains(@onclick, "fileDataDown")]'
        )
        if not download:
            raise Exception("파일 수집 실패")

    # 수집 종료
    complete = collector.end_scraping()
    if not complete:
        raise Exception("파일 다운로드 시간초과")


def refine():
    # 수집된 파일목록
    download_files = file_utils.get_all_files(collected_dir, extension=".zip")

    # 압축 해제
    for file_path in download_files:
        save_path = Path(str(file_path).replace(str(collected_dir), str(refined_dir)))
        file_utils.extract_zip_with_structure(file_path, save_path.parent)


def make_shp():
    # 수집한 shp 파일 집합
    shp_files = file_utils.get_all_files(refined_dir, extension=".shp")

    # 컬럼명 변경
    rename_map = {
        "ID": "P_ID",
        "UID": "UID",
        "CLSF_NM": "FRCL_NM",
        "CLSF_CD": "FRCL_CD",
        "STDG_CD": "STDG_CD",
        "STDG_ADDR": "STDG_ADDR",
        "PNU": "RPRS_PNU",
        "LDCG_CD": "RPRS_LDCG",
        "SB_PNU": "SUBPNU",
        "SB_LDCG_CD": "SUBLDCG",
        "AREA": "FRLN_SFC",
        "CAD_CON_RA": "LGSMTRIFF",
        "SOURCE_NM": "INTPR_VIDO",
        "SOURCE_CD": "INTVOD_CD",
        "FLIGHT_YMD": "VIDPOT_YR",
        "UPDT_YMD": "UPDT_YMD",
        "UPDT_TP_NM": "UPTY",
        "UPDT_TP_CD": "UPTY_CD",
        "CHG_RSN_NM": "CHG_PO",
        "CHG_RSN_CD": "CHG_PO_CD",
        "FL_ARMT_YN": "REFRLN_YN",
        "O_UID": "BFI_UID",
        "O_CLSF_NM": "BFIFRCL_NM",
        "geometry": "GEOM",
    }

    # 최종 shp 구축
    for file_path in shp_files:
        gdf = file_utils.read_shapefile(
            file_path,
            columns=list(rename_map.keys()),
            rename_map=rename_map,
            geometry_col="GEOM",
        )
        save_path = Path(str(file_path).replace(str(refined_dir), str(output_dir)))
        file_utils.export_to_shapefile(gdf, save_path)


def run():
    logger.info("%s: 자동화 프로세스 시작", Path(__file__).stem.upper())
    try:
        collect()
        refine()
        make_shp()
        logger.info("%s: 자동화 프로세스 완료", Path(__file__).stem.upper())
    except Exception as e:
        logger.error("%s: 자동화 실패", Path(__file__).stem.upper())
        logger.error("%s - %s", type(e).__name__, str(e), exc_info=True)


if __name__ == "__main__":
    run()
