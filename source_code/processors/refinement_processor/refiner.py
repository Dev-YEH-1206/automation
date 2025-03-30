import re
from pathlib import Path
from typing import Dict, List, Tuple

import geopandas as gpd
import pandas as pd

import source_code.utils.file_util as file_utils
import source_code.utils.string_util as string_utils
from source_code.config.logging_config import setup_logger

logger = setup_logger(__name__.split(".")[-1])


class Refiner:
    def __init__(self):
        pass

    def refine_addr(
        self, df: pd.DataFrame, addr_cols: List[str] = ["ADDR"]
    ) -> pd.DataFrame:
        """
        주어진 DataFrame에서 주소 열을 정제하여 새로운 열 'REFADDR'를 추가한다.

        Args:
            df (pd.DataFrame): 주소가 포함된 DataFrame
            addr_cols (List[str], 선택): 주소가 포함된 열의 리스트, 기본값은 ['ADDR']

        Returns:
            pd.DataFrame: 'REFADDR'열에 정제주소 데이터가 추가된 DataFrame
        """

        def get_refined_addr(row: pd.Series, addr_cols: List[str]) -> str:
            """
            주어진 행에서 유효한 주소를 추출하여 정제된 주소를 반환한다
            """
            for entry in addr_cols:
                try:
                    addr = row[entry]
                    if string_utils.is_valid_string(addr):
                        return apply_addr_refinement_logic(addr)
                except KeyError:
                    addr_cols.remove(entry)
            return ""

        def apply_addr_refinement_logic(addr: str):
            """
            주소 데이터에 정제 로직을 적용한다.
            """
            # 1. Nested 괄호: 가장 안쪽의 괄호만 남기고 삭제
            addr = re.sub(r"\(([^)]*)\(", "(", addr)
            addr = re.sub(r"\)([^(]*)\)", ")", addr)

            # 2. 단일 괄호: 삭제
            addr = re.sub(r"\(.*?\)", "", addr)

            # 3. 쉼표: 공백처리
            addr = addr.replace(",", " ")

            # 4. 우편번호로 시작: 삭제
            addr = re.sub(r"^\d+[-\d]*", "", addr)

            # 4. 주소정제
            pattern = re.compile(
                r"(?P<do>\S+)?\s*"
                r"(?P<si>\S+시\s+)?\s*"
                r"(?P<gun>\S+군\s+)?\s*"
                r"(?P<gu>\S+구\s+)?\s*"
                r"(?P<eup>\S+읍\s+)?\s*"
                r"(?P<myeon>\S+면\s+)?\s*"
                r"(?P<dong>\S+동\s+)?\s*"
                r"(?P<tong>\S+통\s+)?\s*"
                r"(?P<li>\S+리\s+)?\s*"
                r"(?P<ro>\S*로)?\s*"
                r"(?P<gil>\S*길)?\s*"
                r"(?P<san>산)?"
                r"(?P<num>\d+[-\d]*(번지)?)?\s*"
            )

            addr = addr.strip()
            match = pattern.match(addr)
            if match:
                groups = match.groupdict()
                # 불필요한 공백 제거
                if groups["ro"]:
                    groups["ro"] = groups["ro"].replace(" ", "")
                if groups["gil"]:
                    groups["gil"] = groups["gil"].replace(" ", "")

                # 주소 구성 요소 병합
                groups["road"] = (groups["ro"] or "") + (groups["gil"] or "")
                components = [
                    "do",
                    "si",
                    "gun",
                    "gu",
                    "eup",
                    "myeon",
                    "dong",
                    "tong",
                    "li",
                    "road",
                    "san",
                    "num",
                ]
                addr = " ".join(
                    groups[component] for component in components if groups[component]
                )
                addr = re.sub(r"\s+", " ", addr)  # 연속된 공백 제거
                addr = re.sub(r"·", "", addr)  # · 제거 안하면 3·15대로 지오코딩 실패
            return addr  # 주소 반환

        # DataFrame의 각 행에 대해 정제된 주소를 추가
        df["REFADDR"] = df.apply(lambda row: get_refined_addr(row, addr_cols), axis=1)

        return df

    def export_refined_addr(
        self,
        df: pd.DataFrame,
        dir_path: Path,
        index_label: str = "addr_idx",
        chunk_size: int = 50_000,
        engine="xlsxwriter",
    ) -> None:
        """
        주어진 DataFrame에 저장된 정제 주소를 엑셀 파일로 저장한다.

        IMPORTANT:
            - 공간빅데이터 분석플랫폼의 지오코딩 처리 용량은 1회당 최대 50_000건.
              따라서 정제주소를 50_000건 이하로 나누어 저장해야 함.
            - 'xlsxwriter' 이외의 엔진을 사용하면 오류 발생. (공간빅데이터 분석플랫폼에서 빈 엑셀 파일로 인식)

        Args:
            df (pd.DataFrame): 정제된 주소를 포함하는 DataFrame
            dir_path (Path): 엑셀 파일을 저장할 디렉토토리 경로
            index_label (str): 인덱스 레이블 (기본값은 "addr_idx")
            chunk_size (int): 각 청크의 크기 (기본값은 50_000)

        Returns:
            None: 엑셀 파일 저장
        """
        # 정제된 주소를 청크 단위로 저장
        chunks = file_utils.chunk_dataframe(df, chunk_size=chunk_size)
        for idx, chunk in enumerate(chunks):
            file_utils.export_to_xlsx(
                chunk,
                Path(dir_path / "CHUNK", f"chunk_{idx}.xlsx"),
                columns=["REFADDR"],
                index=True,
                index_label=index_label,
                engine=engine,
            )

    def sjoin_with_boundary(
        self,
        gdf: gpd.GeoDataFrame,
        bnd_gdfs: Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame],
        how: str = "left",
        predicate: str = "within",
    ) -> gpd.GeoDataFrame:
        """
        주어진 GeoDataFrame과 행정구역경계 GeoDataFrame을 공간조인한다.

        Args:
            gdf (gpd.GeoDataFrame): 대상 GeoDataFrame
            bnd_gdf (Tuple[gpd.GeoDataFrame, ...]): 행정구역경계 GeoDataFrame 튜플
            how (str, optional): 조인 방법 (기본값: 'left')
            predicate (str, optional): 공간 조인 기준 (기본값: 'within')

        Returns:
            gpd.GeoDataFrame: 공간 조인이 완료된 GeoDataFrame
        """

        def clean_sjoin(
            gdf_left: gpd.GeoDataFrame,
            gdf_right: gpd.GeoDataFrame,
            how=how,
            predicate=predicate,
        ):
            """
            공간 조인 후 불필요한 '_left' 및 '_right' 컬럼을 삭제한다.

            Args:
                gdf_left (gpd.GeoDataFrame): 공간 조인할 GeoDataFrame
                gdf_right (gpd.GeoDataFrame): 공간 조인할 GeoDataFrame

            Returns:
                gpd.GeoDataFrame: 불필요한 컬럼이 제거된 GeoDataFrame
            """
            sjoined_gdf = gpd.sjoin(gdf_left, gdf_right, how=how, predicate=predicate)
            return sjoined_gdf.drop(
                columns=[
                    col
                    for col in sjoined_gdf.columns
                    if col.endswith(("_left", "_right"))
                ],
                errors="ignore",
            )

        # crs 통일
        target_crs = "EPSG:5179"
        gdf = gdf.to_crs(target_crs)
        bnd_gdfs = [bnd.to_crs(target_crs) for bnd in bnd_gdfs]

        # 행정구역경계 공간조인
        for bnd in bnd_gdfs:
            gdf = clean_sjoin(gdf, bnd)

        return gdf

    def check_note(self, gdf: gpd.GeoDataFrame, action: str) -> gpd.GeoDataFrame:
        """
        action이 'include'인 경우 'note'가 적힌 행만 반환한다.
        action이 'exclude'인 경우 'note'가 적히지 않은 행만 반환한다.

        Args:
            gdf (gpd.GeoDataFrame): 'note'를 확인할 GeoDataFrame
            action (str): 'note' 값을 포함할지 제외할지 선택

        Returns:
            gpd.GeoDataFrame: 필터링된 GeoDataFrame
        """
        if "note" not in gdf.columns:
            return None

        # 유효한 노트가 적힌 데이터
        filtered_gdf = gdf[gdf["note"].notna() & (gdf["note"] != "")]

        if action == "include":
            return filtered_gdf
        elif action == "exclude":
            return gdf.drop(index=filtered_gdf.index).drop(columns=["note"])
        else:
            return None

    def data_filtering(
        self,
        df: pd.DataFrame,
        inclusion: Dict[str, List[str]] = {},
        exclusion: Dict[str, List[str]] = {},
    ) -> pd.DataFrame:
        """
        주어진 DataFrame에서 inclusion 및 exclusion 조건에 맞는 데이터를 필터링한다.

        inclusion 조건이 주어지면 해당 조건을 만족하는 행만 남기고,
        exclusion 조건이 주어지면 해당 조건을 만족하는 행을 제외한다.

        Args:
            df (pd.DataFrame): 필터링할 DataFrame
            inclusion (Dict): 포함할 값을 정의한 딕셔너리 (기본값: 빈 딕셔너리)
            exclusion (Dict): 제외할 값을 정의한 딕셔너리 (기본값: 빈 딕셔너리)

        Returns:
            pd.DataFrame: 필터링된 새로운 DataFrame
        """

        def apply_inclusion(row, inclusion: Dict[str, List[str]]):
            """
            주어진 행의 특정 컬럼 값이 inclusion 딕셔너리에 정의된 조건에 맞는지 확인한다.

            inclusion 딕셔너리는 컬럼명을 key로, 해당 컬럼이 포함해야 할 값의 리스트를 value로 가진다.
            이 함수는 행의 값이 포함 조건에 맞는 경우 True를 반환하고, 그렇지 않으면 False를 반환합니다.

            Args:
                row (pandas.Series): 확인할 데이터를 담고 있는 DataFrame의 한 행
                exclusion (Dict): 컬럼명을 key로, 포함해야 할 값을 value로 갖는 딕셔너리

            Returns:
                bool: 포함 조건에 맞는 값이 있으면 True, 그렇지 않으면 False
            """
            for col_name, values in inclusion.items():
                if not any(
                    re.search(
                        f"{re.escape(value).replace('%', '.*')}", str(row[col_name])
                    )
                    for value in values
                ):
                    return False
            return True

        def apply_exclusion(row, exclusion: Dict[str, List[str]]) -> bool:
            """
            주어진 행의 특정 컬럼 값이 exclusion 딕셔너리에 정의된 조건에 맞는지 확인한다.

            exclusion 딕셔너리는 컬럼명을 key로, 해당 컬럼에서 제외해야 할 값의 리스트를 value로 가진다.
            이 함수는 행의 값이 제외 조건에 맞는 경우 `True`를 반환하고, 그렇지 않으면 `False`를 반환합니다.

            Args:
                row (pandas.Series): 확인할 데이터를 담고 있는 DataFrame의 한 행
                exclusion (Dict): 컬럼명을 key로, 제외할 값을 value로 갖는 딕셔너리

            Returns:
                bool: 제외 조건에 맞는 값이 있으면 True, 그렇지 않으면 False
            """
            for col_name, values in exclusion.items():
                if any(
                    re.search(
                        f"{re.escape(value).replace('%', '.*')}", str(row[col_name])
                    )
                    for value in values
                ):
                    return True
            return False

        # 데이터프레임 복사본
        filtered_df = df.copy()

        # inclusion 필터 적용
        if inclusion:
            inclusion_mask = filtered_df.apply(
                lambda row: apply_inclusion(row, inclusion), axis=1
            )
            filtered_df = filtered_df[inclusion_mask]

        # exclusion 필터 적용
        if exclusion:
            exclusion_mask = filtered_df.apply(
                lambda row: apply_exclusion(row, exclusion), axis=1
            )
            filtered_df = filtered_df[~exclusion_mask]

        return filtered_df

    def change_columns(
        self,
        df: pd.DataFrame,
        columns: List = None,
        rename_map: Dict = {},
        geometry_col: str = "GEOM",
    ) -> pd.DataFrame:
        """
        DataFrame의 컬럼 순서 및 이름을 변경한다.
        GeoDataFrame이 주어진 경우 지오메트리 컬럼을 설정한다.

        Parameters:
            df (pd.DataFrame): 변환할 DataFrame 또는 GeoDataFrame
            columns (List, optional): 선택할 컬럼 목록 (기본값은 None, 모든 컬럼 유지)
            rename_map (Dict, optional): 컬럼명을 변경할 매핑 딕셔너리 (e.g., {"old_name": "new_name"})
            geometry_col (str, optional): GeoDataFrame인 경우 사용할 지오메트리 컬럼명 (기본값은 "GEOM")

        Returns:
            pd.DataFrame: 컬럼 순서 및 이름이 변경된 DataFrame 또는 GeoDataFrame


        """
        if columns:
            df = df.loc[:, columns]

        if rename_map:
            df.rename(columns=rename_map, inplace=True)
            if isinstance(df, gpd.GeoDataFrame):
                df.set_geometry(geometry_col, inplace=True)

        return df
