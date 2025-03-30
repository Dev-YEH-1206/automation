import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

import chardet
import geopandas as gpd
import pandas as pd
from dbfread import DBF

import source_code.config.paths as paths
import source_code.utils.string_util as string_utils
from source_code.config.logging_config import setup_logger
from source_code.processors.refinement_processor.refiner import Refiner

logger = setup_logger(__name__.split(".")[-1])


def extract_zip_with_structure(zip_path: Path, destination_dir: Path) -> List[Path]:
    """
    원본 zip 파일의 디렉토리 구조를 유지하면서 압축을 해제한 후, 원본 zip 파일을 삭제한다.

    Args:
        zip_path (Path): 원본 zip 파일의 경로.
        destination_dir (Path): 압축 해제된 파일이 저장될 경로.

    Returns:
        List[Path]: 압축 해제된 파일의 경로 목록
    """

    # zip 파일인지 확인
    if zip_path.suffix.lower() != ".zip":
        return []

    # 압축 해제된 파일
    extracted_files = []

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for file_info in zip_ref.infolist():
            try:
                file_name = detect_filename_encoding(file_info.filename)
                destination_path = (
                    destination_dir / zip_path.stem.split(".", 1)[0] / file_name
                )

                if file_info.is_dir():
                    # 디렉토리 생성
                    destination_path.mkdir(parents=True, exist_ok=True)
                else:
                    # 부모 디렉토리 생성
                    destination_path.parent.mkdir(parents=True, exist_ok=True)
                    # 파일 저장
                    with zip_ref.open(file_info) as source, open(
                        destination_path, "wb"
                    ) as target:
                        target.write(source.read())
                    extracted_files.append(destination_path)

            except Exception as e:
                logger.error("압축 해제 실패: %s", zip_path.stem)
                return []

    logger.info("압축 해제 성공: %s", zip_path.stem)
    return extracted_files


def detect_filename_encoding(filename: str) -> str:
    """
    zipfile 라이브러리를 활용해 압축을 해제할 때
    파일명이 깨지지 않도록 인코딩을 감지한다.

    IMPORTANT:
        '보건복지부'에서 수집한 '지역보건의료기관' 압축파일 해제 시,
        파일명이 깨지는 현상이 발생하여 해당 함수를 작성함

    Args:
        filename (str): 압축파일 내의 파일명

    Returns:
        str: 인코딩이 감지된 파일명
    """
    try:
        encoded_filename = filename.encode("cp437")
    except UnicodeEncodeError:
        # 인코딩 에러가 발생하지 않으면 filename 그대로 사용
        return filename

    encodings = ["euc-kr", "utf-8", "cp949"]  # 수동감지
    while encodings:
        encoding = encodings.pop()
        try:
            decoded = encoded_filename.decode(encoding)
            return decoded
        except (UnicodeDecodeError, UnicodeEncodeError):
            continue

    encoded_filename = filename.encode("cp437")
    detected = chardet.detect(encoded_filename)  # 자동감지 결과

    # 0.6 이상의 확률이면 감지된 인코딩을 활용하고, 아니면 'cp949' 활용
    return (
        encoded_filename.decode(detected["encoding"])
        if detected["confidence"] > 0.6
        else encoded_filename.decode("cp949")
    )


def read_excel(
    file_path: Path,
    sheet_keyword: str = None,
    header_cols: List = None,
    rename: List = None,
    **kwargs,
) -> pd.DataFrame:
    """
    엑셀 파일을 읽어 pandas DataFrame으로 반환한다.

    기본적으로 첫 번째 시트를 읽으며, 옵션을 통해 특정 시트를 선택하거나 컬럼명을 변경할 수 있다.

    Args:
        file_path (Path): 읽을 엑셀 파일의 경로
        sheet_keyword (str, 선택): 시트명이 포함해야 할 키워드
        header_cols (list, 선택): 헤더가 포함해야 할 키워드
        rename (list, 선택): 변경된 컬럼명 리스트
        **kwargs:
            - sheet_name (int 또는 str, 기본값=0): 읽을 시트의 인덱스 또는 이름
            - usecols (list 또는 str, 선택): 읽을 열을 지정

    Returns:
        pd.DataFrame: 엑셀 데이터를 포함하는 DataFrame
    """
    # xls 파일의 엔진 설정
    if file_path.suffix.lower() == ".xls":
        kwargs["engine"] = "xlrd"

    # 시트 선택
    if sheet_keyword:
        kwargs["sheet_name"] = _find_sheet_with_keywords(file_path, sheet_keyword)

    # 헤더 감지
    temp_df = pd.read_excel(
        file_path,
        dtype=str,
        nrows=30,
        header=None,
        sheet_name=kwargs.get("sheet_name", 0),
    )
    if header_cols:
        if _find_header(temp_df, header_cols) is False:  # 헤더 감지 실패
            return pd.DataFrame()
        kwargs["header"], kwargs["usecols"] = _find_header(temp_df, header_cols)

    # 데이터프레임 생성
    df: pd.DataFrame = pd.read_excel(file_path, dtype=str, **kwargs)

    # 컬럼명 변경
    if rename:
        rename_map = {original: new for original, new in zip(kwargs["usecols"], rename)}
        df.rename(columns=rename_map, inplace=True)

    return df


def read_shapefile(
    file_path: Path,
    columns: List = None,
    rename_map: Dict = None,
    geometry_col: str = "geometry",
) -> gpd.GeoDataFrame:
    """
    주어진 Shapefile을 읽고 인코딩을 감지하여 GeoDataFrame으로 반환한다.

    Args:
        file_path (Path): Shapefile의 경로
        columns (list, optional): Shapefile에서 읽을 컬럼의 리스트. 기본값은 모든 컬럼
        rename_map (Dict, optional): 컬럼명 딕셔너리 (기본값: None, 기존 컬럼명 사용)
        geometry_col (str, "geometry"): 공간정보 컬럼 (기본값: 'geometry')

    Returns:
        gpd.GeoDataFrame: Shapefile 데이터로 생성한 GeoDataFrame
    """
    encoding = detect_shp_encoding(file_path)
    if not encoding:
        return gpd.GeoDataFrame()

    gdf = gpd.read_file(file_path, encoding=encoding, columns=columns, engine="pyogrio")
    if rename_map:
        refiner = Refiner()
        gdf = refiner.change_columns(
            gdf,
            columns=list(rename_map.keys()),
            rename_map=rename_map,
            geometry_col=geometry_col,
        )

    return gdf


def detect_shp_encoding(file_path: Path) -> Union[str, bool]:
    """
    주어진 shp 파일의 dbf 파일을 읽고 인코딩을 감지한다.

    Args:
        file_path (Path): 인코딩을 감지할 shp 파일의 경로

    Returns:
        Union[str, bool]: 감지된 인코딩 또는 False
    """
    encodings = ["utf-8", "cp949"]
    while encodings:
        encoding = encodings.pop()
        try:
            data = DBF(file_path.with_suffix(".dbf"), encoding=encoding)
            pd.DataFrame(iter(data))
            logger.info("%s 인코딩 확인: %s", file_path.name, encoding)
            return encoding
        except (UnicodeDecodeError, UnicodeEncodeError):
            continue
        except FileNotFoundError:
            logger.error("dbf파일이 존재하지 않습니다: %s", file_path.name)
            return False
    return False


def _find_header(df: pd.DataFrame, header_cols: List) -> Union[tuple, bool]:
    """
    모든 'header_cols'를 포함하고 있는 행을 찾아
    해당 행의 인덱스와 컬럼명 리스트를 반환한다.

    Args:
        df (pd.DataFrame): 헤더 행을 찾을 DataFrame.
        header_cols (list): 헤더가 포함해야 하는 컬럼명 리스트.

    Returns:
        Union[tuple, bool]: 헤더 행을 찾은 경우에는 다음을 포함하는 튜플을 반환하고, 그렇지 않으면 False
            - int: 'header_cols'의 모든 값이 포함된 행의 인덱스.
            - list: 해당 행에서 'header_cols'와 일치하는 컬럼명 리스트.
    """
    cleaned_header_cols = [string_utils.clean_text(str(val)) for val in header_cols]

    for row_index, row_values in enumerate(df.itertuples(index=False, name=None)):
        cleaned_row = [string_utils.clean_text(str(val)) for val in row_values]

        # header_cols 포함여부 확인
        if all(header_col in cleaned_row for header_col in cleaned_header_cols):
            usecols = [
                next(
                    row_val
                    for row_val in row_values
                    if string_utils.clean_text(str(row_val)) == header_col
                )
                for header_col in cleaned_header_cols
            ]
            return row_index, usecols
    logger.error(f"{header_cols}를 포함하는 헤더 행을 찾을 수 없습니다")
    return False


def _find_sheet_with_keywords(file_path: Path, sheet_keyword: str) -> Union[str, int]:
    """
    주어진 엑셀 파일을 읽고, sheet_keyword를 포함하는 시트의 이름를 반환한다.

    Args:
        file_path (Path): 엑셀 파일의 경로.
        sheet_keyword (str): 시트명이 포함해야 할 문자열.

    Returns:
        Union[str, int]: sheet_keyword를 포함하는 시트의 이름 또는 0.
    """
    df_dict = pd.read_excel(file_path, sheet_name=None, nrows=30)
    sheet_names = df_dict.keys()

    matching_sheets = {
        sheet
        for sheet in sheet_names
        if string_utils.check_keywords(sheet, sheet_keyword)
    }

    if len(matching_sheets) == 0:
        logger.warning(
            "주어진 키워드(%s)를 포함하는 시트를 찾지 못하여, 첫번째 시트를 읽습니다",
            sheet_keyword,
        )
        return 0

    if len(matching_sheets) > 1:
        logger.warning(
            "주어진 키워드(%s)를 포함하는 시트가 많아, 첫번째 시트를 읽습니다",
            sheet_keyword,
        )
        return 0

    return next(iter(matching_sheets))


def get_all_files(dir_path: Path, extension: Optional[str] = None) -> Set[Path]:
    """
    주어진 디렉토리와 모든 하위 디렉토리에서 파일을 찾아 경로를 반환한다.

    Args:
        dir_path (Path): 검색할 최상위 디렉토리 경로
        extension (Optional[str]): 검색할 파일 확장자 (e.g., '.xlsx', '.shp')
                                   기본값: None, 모든 확장자

    Returns:
        Set[Path]: 디렉토리 내 모든 파일의 경로 집합
    """
    return {
        file
        for file in dir_path.rglob("*")
        if file.is_file() and (extension is None or file.suffix.lower() == extension)
    }


def find_file_path(files: set[Path], **kwargs) -> Union[Path, bool]:
    """
    주어진 파일 목록에서 특정 파일을 찾아 반환한다.

    Args:
        files (set[Path]): 검색할 파일 경로들의 집합
        **kwargs:
            - extension (str, 선택): 찾을 파일의 확장자 (e.g., '.xlsx', '.shp')
            - file_name (str, 선택): 파일명에 포함될 키워드

    Returns:
        Union[Path, bool]: 파일을 찾은 경우에는 파일의 경로, 그렇지 않으면 False
    """
    # 찾아야 할 파일의 특성
    extension = kwargs.get("extension", "").lower()
    file_name = kwargs.get("file_name", "")

    # 찾아낸 파일의 집합
    matching_files = {
        file
        for file in files
        if file.suffix.lower() == extension
        and string_utils.check_keywords(file.stem, file_name)
    }

    if not matching_files:
        logger.warning("조건을 만족하는 파일을 찾을 수 없습니다.")
        return False

    sorted_files = sorted(matching_files)
    if len(sorted_files) > 1:
        logger.warning(
            "여러 개의 파일이 발견되어 첫 번째 파일을 활용합니다: %s",
            sorted_files[0].stem,
        )

    return sorted_files[0]


def export_to_xlsx(
    df: pd.DataFrame, file_path: Path, index=False, index_label="addr_idx", **kwargs
) -> None:
    """
    주어진 DataFrame을 엑셀 파일로 저장한다.

    Args:
        df (pd.DataFrame): 엑셀 파일로 저장할 DataFrame
        file_path (Path): 엑셀 파일 경로
        index (bool, 기본값=False): 인덱스를 엑셀 파일에 포함할지 여부
        index_label (str, 선택, 기본값="addr_idx"): 인덱스 레이블
        **kwargs:
            - columns (list 또는 str, 선택): 저장할 열을 지정
            - engine (str, 선택): 사용할 엔진

    Returns:
        None: 지정된 경로에 엑셀 파일 생성
    """
    # 부모 디렉토리 생성
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(file_path, index=index, index_label=index_label, **kwargs)

    logger.info("엑셀파일 저장 완료: %s", file_path.name)


def chunk_dataframe(df: pd.DataFrame, chunk_size=50_000) -> List[pd.DataFrame]:
    """
    주어진 DataFrame을 지정된 크기만큼 분할하여 리스트로 반환한다.

    Args:
        df (pd.DataFrame): 분할할 DataFrame
        chunk_size (int): 각 청크의 크기 (기본값은 50_000)

    Returns:
        list: 분할된 DataFrame 리스트트
    """
    return [df[i : i + chunk_size] for i in range(0, len(df), chunk_size)]


def merge_gdf(gdf_list: List[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """
    여러 개의 GeoDataFrame을 하나로 병합한다.

    Args:
        gdf_list (List[gpd.GeoDataFrame]): 병합할 GeoDataFrame 리스트

    Returns:
        gpd.GeoDataFrame: 병합된 GeoDataFrame
    """
    return (
        gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True))
        if gdf_list
        else gpd.GeoDataFrame()
    )


def export_to_shapefile(
    gdf: gpd.GeoDataFrame,
    file_path: Path,
    crs: int = 5179,
    encoding: str = "cp949",
):
    """
    GeoDataFrame을 Shapefile로 저장한다.

    Parameters:
        gdf (gpd.GeoDataFrame): 저장할 GeoDataFrame
        filepath (str): Shapefile의 저장 경로
        crs (int): Shapefile의 EPSG 좌표계. (기본값: 5179)
        encoding (str): Shapefile의 인코딩. (기본값: 'cp949')

    Returns:
        None: 지정된 경로에 Shapefile을 저장
    """
    if gdf.empty:
        logger.error("Shp 저장 실패: %s", file_path)
        return False

    # crs 변환
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=crs)
    elif gdf.crs.to_epsg() != crs:
        gdf = gdf.to_crs(epsg=crs)

    # 저장경로 존재 확인
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Shapefile 저장
    gdf.to_file(file_path, encoding=encoding, engine="pyogrio")

    logger.info("Shp 저장 성공: %s/%s", file_path.parent.name, file_path.name)


def get_boundary_gdf(
    bnd_type: str,
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:

    # 빈 gdf 생성
    sido_gdf = gpd.GeoDataFrame(columns=["geometry"], crs="EPSG:5179")
    sgg_gdf = gpd.GeoDataFrame(columns=["geometry"], crs="EPSG:5179")
    emd_gdf = gpd.GeoDataFrame(columns=["geometry"], crs="EPSG:5179")

    if bnd_type.lower() == "juso":
        JUSO_DIR = paths.JUSO_DIR
        sido_gdf = read_shapefile(
            JUSO_DIR / "법정구역시도_TL_SCCO_CTPRVN.shp",
            columns=["CTP_KOR_NM", "CTPRVN_CD", "geometry"],
            rename_map={
                "CTP_KOR_NM": "CTPV_NM",
                "CTPRVN_CD": "CTPV_CD",
                "geometry": "geometry",
            },
            geometry_col="geometry",
        )
        sgg_gdf = read_shapefile(
            JUSO_DIR / "법정구역시군구_TL_SCCO_SIG.shp",
            columns=["SIG_KOR_NM", "SIG_CD", "geometry"],
            rename_map={
                "SIG_KOR_NM": "SGG_NM",
                "SIG_CD": "SGG_CD",
                "geometry": "geometry",
            },
            geometry_col="geometry",
        )
        emd_gdf = read_shapefile(
            JUSO_DIR / "법정구역읍면동_TL_SCCO_EMD.shp",
            columns=["EMD_KOR_NM", "EMD_CD", "geometry"],
            rename_map={
                "EMD_KOR_NM": "EMD_NM",
                "EMD_CD": "EMD_CD",
                "geometry": "geometry",
            },
            geometry_col="geometry",
        )

    elif bnd_type.lower() == "census":
        CENSUS_DIR = paths.CENSUS_DIR
        sido_gdf = read_shapefile(
            CENSUS_DIR / "bnd_sido_00_2023_2Q.shp",
            columns=["SIDO_NM", "SIDO_CD", "geometry"],
            rename_map={
                "SIDO_NM": "CTPV_NM",
                "SIDO_CD": "CTPV_CD",
                "geometry": "geometry",
            },
            geometry_col="geometry",
        )
        sgg_gdf = read_shapefile(
            CENSUS_DIR / "bnd_sigungu_00_2023_2Q.shp",
            columns=["SIGUNGU_NM", "SIGUNGU_CD", "geometry"],
            rename_map={
                "SIGUNGU_NM": "SGG_NM",
                "SIGUNGU_CD": "SGG_CD",
                "geometry": "geometry",
            },
            geometry_col="geometry",
        )
        emd_gdf = read_shapefile(
            CENSUS_DIR / "bnd_dong_00_2023_2Q.shp",
            columns=["ADM_NM", "ADM_CD", "geometry"],
            rename_map={
                "ADM_NM": "EMD_NM",
                "ADM_CD": "EMD_CD",
                "geometry": "geometry",
            },
            geometry_col="geometry",
        )

    return (sido_gdf, sgg_gdf, emd_gdf)
