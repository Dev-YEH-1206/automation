import pandas as pd


def check_keywords(text: str, keywords: str) -> bool:
    """
    keywords를 공백으로 분리한 후, 모든 요소가 text에 존재하는지 확인한다.

    Args:
        text (str): keywords의 요소를 검색할 문자열.
        keywords (List[str]): text가 포함해야 할 키워드.

    Returns:
        bool: keywords의 모든 요소가 text에 존재하면 True, 아니면 False.
    """
    cleaned_text = clean_text(text)
    splitted_keywords = keywords.split()
    return all(keyword in cleaned_text for keyword in splitted_keywords)


def clean_text(text: str) -> str:
    """
    주어진 문자열에서 공백, 개행문자, 탭을 삭제한다.

    Args:
        text (str): 원본 문자열.

    Returns:
        str: 공백, 개행문자, 탭이 삭제된 문자열.
    """
    return text.translate(str.maketrans("", "", " \t\n"))


def is_valid_string(text: str) -> bool:
    """
    주어진 문자열이 유효한지 확인한다.

    유효한 문자열의 정의:
        - None 또는 NaN이 아님
        - 적어도 하나의 공백이 아닌 문자를 포함함

    Args:
        text (str): 확인할 입력 문자열

    Returns:
        bool: 문자열이 유효하면 True, 그렇지 않으면 False
    """
    return pd.notna(text) and text.strip() != ""
