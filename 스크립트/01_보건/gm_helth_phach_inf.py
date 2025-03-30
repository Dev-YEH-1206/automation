# 보건소및보건의료원(gm_helth_phach_inf)

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
import source_code.utils.string_util as string_utils
from source_code.config import paths
from source_code.config.logging_config import setup_logger
from source_code.processors.collection_processor.selenium_scraper import SeleniumScraper
from source_code.processors.geocoding_processor.geocoder import Geocoder
from source_code.processors.refinement_processor.refiner import Refiner


def collect():
    # 셀레니움 스크레이퍼 인스턴스 생성
    collector = SeleniumScraper(collected_dir)

    # 페이지 접속
    connected = collector.fetch_page(
        "https://www.mohw.go.kr/board.es?mid=a10412000000&bid=0020"
    )
    if not connected:
        raise Exception("페이지 접속 실패")

    # 검색어 입력
    send_keyword = collector.send_key('//input[@name="keyWord"]', "지역보건의료기관")
    if not send_keyword:
        raise Exception("검색어 입력 실패")

    # 검색 버튼 클릭
    searched = collector.click_element(
        '//*[@id="srhForm"]//button[contains(text(), "검색")]'
    )
    if not searched:
        raise Exception("검색 실패")

    # 최신 글 조회
    click_latest = collector.click_element(
        '//*[@id="contents_body"]//tbody/tr[1]//a[contains(text(), "주소록") and contains(text(), "지역보건의료기관")]'
    )
    if not click_latest:
        raise Exception("최신 글 조회 실패")

    # 파일 수집
    download = collector.file_download(
        xpath='//*[@id="contents_body"]/article/div[2]/ul/li/span[2]/a'
    )
    if not download:
        raise Exception("파일 수집 실패")

    # 수집 종료
    collector.end_scraping()


def refine():
    # 수집된 파일목록
    download_files = file_utils.get_all_files(collected_dir)

    # zip 파일 찾기
    zip_file = file_utils.find_file_path(
        download_files, extension=".zip", file_name="지역 보건 의료"
    )
    if not zip_file:
        raise Exception("해당 zip파일 없음")

    # 압축 해제
    extracted = file_utils.extract_zip_with_structure(zip_file, zip_file.parent)

    # 엑셀 파일 찾기
    file_path = file_utils.find_file_path(
        extracted, extension=".xlsx", file_name="지역 보건 의료"
    )
    if not file_path:
        raise Exception("해당 엑셀 파일 없음")

    # 데이터 추출
    df = file_utils.read_excel(
        file_path,
        sheet_keyword="보건소 보건의료원",
        header_cols=["보건기관명", "주소"],
        rename=["PBHLTH_NM", "ADDR"],
    )
    if df.empty:
        raise Exception("데이터프레임 생성 실패")

    # '보건기관구분' 컬럼 추가
    df["HMO_SE"] = df["PBHLTH_NM"].apply(
        lambda x: (
            "보건소"
            if string_utils.clean_text(x).endswith("보건소")
            else (
                "보건의료" if string_utils.clean_text(x).endswith("보건의료원") else ""
            )
        )
    )

    # '보건기관유형' 컬럼 추가
    df["HMO_TYPE"] = df["PBHLTH_NM"].apply(
        lambda x: (
            "보건소"
            if string_utils.clean_text(x).endswith("보건소")
            else (
                "보건의료원"
                if string_utils.clean_text(x).endswith("보건의료원")
                else ""
            )
        )
    )

    # 주소 정제
    refiner = Refiner()
    df = refiner.refine_addr(df, addr_cols=["ADDR"])
    if df.empty:
        raise Exception("주소 정제 실패")

    # 정제된 주소를 엑셀 파일로 저장
    refiner.export_refined_addr(df, refined_dir)

    # 전체 결과물 저장
    columns = ["PBHLTH_NM", "ADDR", "HMO_SE", "HMO_TYPE", "REFADDR"]
    file_utils.export_to_xlsx(
        df, refined_dir / "정제결과물.xlsx", columns=columns, index=True
    )


def geocoding():
    # 공간빅데이터 분석플랫폼 접속
    geocoder = Geocoder(geocoding_dir)
    connected = geocoder.connect_to_platform()
    if not connected:
        raise Exception("공간빅데이터 분석플랫폼 접속 실패")

    # chunk 파일 지오코딩
    chunk_dir = refined_dir / "CHUNK"
    chunks = file_utils.get_all_files(chunk_dir)
    for chunk in chunks:
        geocoded = geocoder.start_geocoding(chunk)
        if not geocoded:
            raise Exception("공간빅데이터 분석플랫폼 지오코딩 실패")

    # 지오코딩 결과물 압축 해제
    zip_files = file_utils.get_all_files(geocoding_dir, extension=".zip")
    for file in zip_files:
        file_utils.extract_zip_with_structure(file, file.parent)


def make_shp():
    # refiner 인스턴스 생성
    refiner = Refiner()

    # 행정구역경계 shp
    bnd_gdfs = file_utils.get_boundary_gdf("juso")

    # 정제 데이터
    df = file_utils.read_excel(refined_dir / "정제결과물.xlsx")

    # 최종 shp 리스트
    gdf_list = []

    # 최종 컬럼 순서 및 이름
    rename_map = {
        "CTPV_NM": "CTPV_NM",
        "CTPV_CD": "CTPV_CD",
        "SGG_NM": "SGG_NM",
        "SGG_CD": "SGG_CD",
        "EMD_NM": "EMD_NM",
        "EMD_CD": "EMD_CD",
        "PBHLTH_NM": "PBHLTH_NM",
        "ADDR": "ADDR",
        "REFADDR": "REFADDR",
        "HMO_SE": "HMO_SE",
        "HMO_TYPE": "HMO_TYPE",
        "geometry": "GEOM",
        "note": "note",
    }

    # 최종 shp 생성 시작
    shp_files = file_utils.get_all_files(geocoding_dir, ".shp")
    for idx, shp_file in enumerate(shp_files):
        # 정제주소 shp
        gdf = file_utils.read_shapefile(
            shp_file,
            columns=["addr_idx", "REFADDR", "note", "geometry"],
        )

        # 행정구역경계와 공간조인
        gdf = refiner.sjoin_with_boundary(gdf, bnd_gdfs)

        # shp에 정제데이터 merge
        merged_gdf = gdf.merge(df, on=["addr_idx", "REFADDR"], how="left")
        gdf_list.append(merged_gdf)

        # 컬럼 순서 변경
        merged_gdf = refiner.change_columns(
            merged_gdf,
            columns=list(rename_map.keys()),
            rename_map=rename_map,
            geometry_col="GEOM",
        )

        # 최종 shp 구축 시작
        # 1. note 있는 shp
        note_shp = refiner.check_note(merged_gdf, action="include")
        if note_shp is None:
            pass
        elif note_shp.empty:
            logger.info("note가 적힌 데이터가 없습니다")
        else:
            note_file_name = f"{Path(__file__).stem.upper()}_chunk_note_{idx}.shp"
            note_path = output_dir / "note" / note_file_name
            file_utils.export_to_shapefile(note_shp, note_path)

        # 2. note 없는 shp
        final_shp = refiner.check_note(merged_gdf, action="exclude")
        if not final_shp.empty:
            final_file_name = f"{Path(__file__).stem.upper()}_chunk_{idx}.shp"
            final_path = output_dir / final_file_name
            file_utils.export_to_shapefile(final_shp, final_path)


def run():
    logger.info("%s: 자동화 프로세스 시작", Path(__file__).stem.upper())
    try:
        collect()
        refine()
        geocoding()
        make_shp()
        logger.info("%s: 자동화 프로세스 완료", Path(__file__).stem.upper())
    except Exception as e:
        logger.error("%s: 자동화 실패", Path(__file__).stem.upper())
        logger.error("%s - %s", type(e).__name__, str(e), exc_info=True)


if __name__ == "__main__":
    run()
