import time
from datetime import datetime
from pathlib import Path
from typing import Union

from selenium import webdriver
from selenium.common.exceptions import (
    NoAlertPresentException,
    NoSuchWindowException,
    TimeoutException,
    UnexpectedAlertPresentException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from source_code.config.logging_config import setup_logger
from source_code.utils.decorators import check_new_file

logger = setup_logger(__name__.split(".")[-1])


class SeleniumScraper:

    TIMEOUT = 30

    def __init__(self, download_dir: Path) -> None:
        self.download_dir = self._set_download_dir(download_dir)
        self.driver: webdriver.Chrome = self._get_chrome_driver()

    def fetch_page(self, url: str, timeout: int = TIMEOUT) -> bool:
        """
        주어진 URL에 접속하여 페이지를 불러온다.

        Args:
            url (str): 접속할 웹페이지 URL
            timeout (int, optional): 페이지 로딩 대기 시간 (기본값: TIMEOUT)

        Returns:
            bool: 페이지가 정상적으로 로드되면 True, 그렇지 않으면 False
        """
        self.driver.get(url)
        if self._wait_for_page_load(timeout=timeout):
            logger.info(f"{url} 페이지 접속 성공")
            return True
        logger.error(f"{url} 페이지 접속 실패")
        return False

    def send_key(
        self,
        xpath: str,
        value: str,
        timeout: int = TIMEOUT,
        msg: str = "검색어 입력 성공",
    ) -> bool:
        """
        주어진 XPath에 해당하는 입력 필드에 값을 입력한다.

        Args:
            xpath (str): 입력할 element의 XPath
            value (str): 입력할 문자열 값
            timeout (int, optional): element 탐색 대기 시간 (기본값: TIMEOUT)

        Returns:
            bool: 입력 성공 시 True, element를 찾지 못하면 False
        """
        input_element = self.find_element(xpath, timeout=timeout)

        if isinstance(input_element, WebElement):
            input_element.send_keys(value)
            logger.info(msg)
            return True
        logger.error(f"send_key 에러: {xpath} 요소에 {value} 입력 실패")
        return False

    def find_element(
        self, xpath: str, timeout: int = TIMEOUT, log: bool = True
    ) -> Union[WebElement, bool]:
        """
        주어진 XPath에 해당하는 첫번째 element를 찾는다.

        Args:
            xpath (str): 찾을 element의 XPath
            timeout (int, optional): element 탐색 대기 시간 (기본값: TIMEOUT)

        Returns:
            Union[WebElement, bool]: element를 찾으면 WebElement 객체, 찾지 못하면 False 반환
        """
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )
            return element
        except TimeoutException:
            if log:
                logger.error(f"첫번째 {xpath} 요소 찾기 실패")
            return False

    def find_all_elements(
        self, xpath: str, timeout: int = TIMEOUT
    ) -> Union[list[WebElement], bool]:
        """
        주어진 XPath에 해당하는 element를 전부 찾는다.

        Args:
            xpath (str): 찾을 element의 XPath
            timeout (int, optional): elements 탐색 대기 시간 (기본값: TIMEOUT)

        Returns:
            Union[list[WebElement], bool]:
                - elements를 찾으면 WebElement 객체들의 리스트 반환
                - elements를 찾지 못하면 False 반환
        """
        try:
            elements = WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_all_elements_located((By.XPATH, xpath))
            )
            return elements
        except TimeoutException:
            logger.error(f"모든 {xpath} 요소 찾기 실패")
            return False

    def click_element(self, xpath: str, timeout: int = TIMEOUT) -> bool:
        """
        주어진 XPath에 해당하는 단일 요소를 클릭한다.

        Args:
            xpath (str): 클릭할 요소의 XPath
            timeout (int, optional): 요소 탐색 대기 시간 (기본값: TIMEOUT)

        Returns:
            bool:
                - 클릭 성공 시 True 반환
                - 요소를 찾지 못하거나 클릭할 수 없으면 False 반환
        """
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
        except TimeoutException:
            logger.error(f"{xpath} 요소 클릭 실패")
            return False

        element.click()
        return self._wait_for_page_load()

    def click_elements(self, xpath: str, timeout: int = TIMEOUT) -> bool:
        """
        주어진 XPath에 해당하는 모든 요소를 순차적으로 클릭한다.

        Args:
            xpath (str): 클릭할 요소들의 XPath
            timeout (int, optional): 요소 탐색 대기 시간 (기본값: TIMEOUT)

        Returns:
            bool:
                - 모든 요소가 클릭되었으면 True 반환
                - 요소를 찾을 수 없거나 클릭에 실패하면 False 반환
        """
        elements = self.find_all_elements(xpath, timeout=timeout)
        if elements:
            for element in elements:
                element.click()
                if not self._wait_for_page_load():
                    logger.error(f"{xpath} 모든 요소 클릭 실패")
                    return False
            return True
        logger.error(f"{xpath} 모든 요소 클릭 실패")
        return False

    def select_by_option_value(
        self, xpath: str, value: str, timeout: int = TIMEOUT
    ) -> bool:
        """
        주어진 XPath에 해당하는 <select> 태그에서 특정 값을 가진 옵션을 선택한다.

        Args:
            xpath (str): <select> 요소의 XPath
            value (str): 선택할 옵션의 값 (value 속성)
            timeout (int, optional): 요소 탐색 대기 시간 (기본값: TIMEOUT)

        Returns:
            bool:
                - 옵션을 선택하면 True 반환
                - 요소를 찾을 수 없거나 옵션 선택에 실패하면 False 반환
        """
        # Select 객체 생성
        select_element = self.find_element(xpath, timeout=timeout)
        if select_element:
            select = Select(select_element)
            try:
                select.select_by_value(value)
                return True
            except Exception as e:
                logger.error(f"{xpath} 요소에서 {value} 선택 실패")
                return False
        logger.error(f"{xpath} 요소에서 {value} 선택 실패")
        return False

    def switch_to_window(
        self, url: str, timeout: int = 10, min_windows: int = 2
    ) -> bool:
        """
        지정된 URL을 포함하는 새 창(탭)으로 전환한다.

        Args:
            url (str): 전환할 창의 URL 일부 문자열
            timeout (int, optional): 창 탐색 대기 시간 (기본값: 10초)
            min_windows (int, optional): 최소 열린 창 개수 (기본값: 2)

        Returns:
            bool:
                - URL을 포함하는 창으로 전환하면 True 반환
                - 지정된 창을 찾지 못하면 False 반환
        """
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda driver: len(driver.window_handles) >= min_windows
            )
            window_handles = self.driver.window_handles
            for handle in window_handles:
                self.driver.switch_to.window(handle)
                if url in self.driver.current_url:
                    return True
            logger.error(f"창 전환 실패: {url}")
            return False
        except TimeoutException:
            logger.error(f"창 전환 실패: {url}")
            return False

    def switch_to_iframe(self, xpath: str, timeout: int = TIMEOUT) -> bool:
        """
        주어진 XPath에 해당하는 iframe 요소로 전환한다.

        Args:
            xpath (str): iframe 요소의 XPath
            timeout (int, optional): 요소 탐색 대기 시간 (기본값: TIMEOUT)

        Returns:
            bool:
                - iframe으로 전환하면 True 반환
                - 요소를 찾지 못하면 False 반환
        """
        iframe = self.find_element(xpath, timeout=timeout)
        if iframe:
            self.driver.switch_to.frame(iframe)
            return True
        logger.error(f"아이프레임 전환 실패: {xpath}")
        return False

    def switch_to_parent_frame(self) -> None:
        """
        현재 iframe에서 parent 프레임으로 전환한다.

        Returns:
            None
        """
        self.driver.switch_to.parent_frame()

    @check_new_file
    def file_download(self, **kwargs):
        """
        주어진 XPath를 클릭하여 파일 다운로드를 시작한다.

        Args:
            **kwargs (dict): 다운로드를 시작할 요소의 XPath를 포함하는 키워드 인수
                             xpath (str): 클릭할 요소의 XPath (필수)
                             timeout (int, 선택): 다운로드를 기다릴 최대 시간 (기본값: 60 * 60초)

        Returns:
            bool: 다운로드 시작 여부. 다운로드가 성공적으로 시작되면 True, 실패하면 False
        """
        # 수집 당시 화면 스크린샷
        self.get_screenshot()

        download_clicked = self.click_element(kwargs.get("xpath", ""))
        if download_clicked:
            logger.info("파일 다운로드 시작")
            return True

        logger.info("파일 다운로드 실패")
        return False

    def get_screenshot(self) -> bool:
        """
        현재 페이지의 스크린샷을 캡쳐하여 다운로드 디렉토리에 저장한다.
        파일명은 현재 날짜와 시간으로 자동 생성된다.

        Returns:
            bool: 화면 캡쳐에 성공하면 True, 실패하면면 False
        """
        now = datetime.now()
        file_name = now.strftime("%Y-%m-%d_%H%M%S") + ".png"
        file_path = self.download_dir / file_name
        if self.driver.get_screenshot_as_file(file_path):
            logger.info("화면 캡쳐 성공")
            return True
        logger.error("화면 캡쳐 실패")
        return False

    def end_scraping(self, timeout: int = 60 * 60) -> None:
        """
        웹드라이버를 종료하기 전에 다운로드가 완료될 때까지 대기한다.

        주어진 시간 내에 다운로드가 완료되지 않으면 False를 반환하고,
        다운로드 완료되는 경우에는 드라이버를 종료하고 True를 반환한다.

        Args:
            timeout (int): 다운로드 완료를 기다릴 최대 시간(초). 기본값은 1시간(60 * 60초)

        Returns:
            bool: 다운로드 완료 후 웹드라이버를 종료하면 True, 그렇지 않으면 False
        """
        # 다운로드 제한 시간 설정
        end_time = time.time() + timeout

        # 다운로드 대기
        while time.time() < end_time:
            # 현재 디렉토리의 임시 파일 목록을 확인
            tmp_files = {
                f
                for f in self.download_dir.glob("*")
                if f.suffix.lower() in (".crdownload", ".tmp")
            }
            if not tmp_files:  # 임시 파일이 없으면 다운로드 완료로 판단
                self.driver.quit()
                return True
            time.sleep(10)  # 잠시 대기 후 다시 확인

        # 타임아웃 후 웹드라이버 종료
        self.driver.quit()
        return False

    def _set_download_dir(self, download_dir: Path) -> Path:
        """
        주어진 다운로드 디렉토리 경로가 존재하는지 확인하고, 없으면 새로 생성한다.

        Args:
            download_dir (Path): 다운로드 파일을 저장할 디렉토리 경로

        Returns:
            None
        """
        # 디렉토리가 존재하지 않으면 생성
        if not download_dir.exists():
            download_dir.mkdir(parents=True, exist_ok=True)
        return download_dir

    def _get_chrome_driver(self) -> webdriver.Chrome:
        """
        크롬 웹드라이버를 생성하고 반환한다.

        Args:
            download_dir: 파일 다운로드 디렉토리

        Returns:
            webdriver.Chrome: 크롬 웹드라이버
        """
        chrome_options = Options()

        # headless 설정
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        # 다운로드 설정
        prefs = {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "safebrowsing.disable_download_protection": True,
            "profile.default_content_settings.popups": 0,
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # 디폴트 로그 제한
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

        # 안전하지 않은 다운로드 허용
        chrome_options.add_argument("--enable-unsafe-swiftshader")
        chrome_options.add_argument(
            "--unsafely-treat-insecure-origin-as-secure=http://geobigdata.go.kr"
        )

        # 화면크기 및 알림 설정
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-notifications")

        # Selenium 로컬 설치
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=chrome_options
        )

        # # Selenium Standalone
        # SELENIUM_SERVER = "http://selenium:4444/wd/hub"
        # driver = webdriver.Remote(
        #     command_executor=SELENIUM_SERVER, options=chrome_options
        # )

        return driver

    def _wait_for_page_load(self, timeout: int = TIMEOUT) -> bool:
        """
        페이지가 완전히 로드될 때까지 대기한다.
        Alert가 뜨는 경우 '예'로 처리한다.

        Args:
            timeout (int): 페이지 로드 대기 시간 (초)

        Returns:
            bool: 페이지가 정상적으로 로드되면 True, 그렇지 않으면 False
        """
        # Alert 확인
        self._yes_to_alert()

        # 페이지가 완전히 로드될 때까지 대기
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            return True
        except TimeoutException:
            pass
        except UnexpectedAlertPresentException:
            self._yes_to_alert()
            return self._wait_for_page_load(timeout)

        return False

    def _yes_to_alert(self):
        """
        Alert에 '예' 응답을 보낸다.
        """
        while True:
            time.sleep(3)
            try:
                alert = self.driver.switch_to.alert
                alert.accept()
            except (NoAlertPresentException, NoSuchWindowException):
                break
