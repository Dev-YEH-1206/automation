import time
from pathlib import Path
from typing import Union

from selenium.webdriver.remote.webelement import WebElement

from source_code.config.logging_config import setup_logger
from source_code.processors.collection_processor.selenium_scraper import SeleniumScraper
from source_code.processors.refinement_processor.refiner import Refiner

logger = setup_logger(__name__.split(".")[-1])


class Geocoder:

    def __init__(self, download_dir: Path):
        self.selenium_scraper = SeleniumScraper(download_dir)

    def connect_to_platform(self):
        # 공간빅데이터 분석 플랫폼 접속
        fetched = self.selenium_scraper.fetch_page(
            "http://geobigdata.go.kr/portal/uat/uia/egovLoginUsr.do"
        )
        if not fetched:
            logger.errror("공간빅데이터 분석플랫폼 접속 실패")
            return False
        logger.info("공간빅데이터 분석플랫폼 접속 성공")

        # 로그인
        self.selenium_scraper.send_key(
            '//input[@name="id"]', "nemesy11@gmail.com", msg="아이디 입력 성공"
        )
        self.selenium_scraper.send_key(
            '//input[@name="password"]', "dbdmsgml1!", msg="비밀번호 입력 성공"
        )
        self.selenium_scraper.click_element('//input[@name="ip_login"]')
        if not self.selenium_scraper.find_element('//a[text()="로그아웃"]'):
            logger.error("공간빅데이터 분석플랫폼 로그인 실패")
            return False
        logger.info("공간빅데이터 분석플랫폼 로그인 성공")
        return True

    def start_geocoding(self, file_path: Path, timeout: int = 60 * 60) -> bool:
        # 정제주소 파일 업로드
        self.upload_xlsx(file_path)

        # Shp 다운로드 시작
        self.selenium_scraper.click_element('//button[text()="Shape 다운로드"]')

        # 진행상황 체크 및 shp 다운로드
        download = self.monitoring_progress(file_path, timeout=timeout)
        if not download:
            logger.error("shp 다운로드 실패")
            return False

        # 창닫기 및 전환
        self.selenium_scraper.driver.close()
        handles = self.selenium_scraper.driver.window_handles
        if handles:
            self.selenium_scraper.driver.switch_to.window(handles[-1])

        return True

    def monitoring_progress(self, file_path: Path, timeout: int = 60 * 60) -> bool:
        # 모니터링 페이지 팝업 확인
        popup = self.selenium_scraper.switch_to_window(
            "geocodingMonitoring", timeout=30
        )
        if not popup:
            self.selenium_scraper.fetch_page(
                "http://geobigdata.go.kr/portal/geomonitoring/geocodingMonitoring.do"
            )

        # 모니터링 페이지 로드 확인
        file_name_td = f'//div[contains(text(), "{file_path.name}")]/..'
        download_xpath = '/following-sibling::td[3]//*[text()="다운로드"]'
        max_retries = 30
        retries = 0
        while retries < max_retries:
            try:
                # 다운로드 버튼 존재 확인
                download_button = self.selenium_scraper.find_element(
                    file_name_td + download_xpath, log=False, timeout=5
                )
                if not download_button:
                    retries += 1
                    if retries > max_retries:
                        logger.error("모니터링 페이지 로드 불가능")
                        return False
                    self.selenium_scraper.driver.refresh()
                else:
                    break
            except Exception:
                retries += 1
                self.selenium_scraper.driver.refresh()
                time.sleep(5)
                if retries > max_retries:
                    logger.error("모니터링 페이지 로드 불가능")
                    return False

        # 다운로드 가능 여부 확인
        while "disabled" in download_button.get_attribute("class"):
            try:
                self.selenium_scraper.driver.refresh()
                time.sleep(10)
                download_button = self.selenium_scraper.find_element(
                    file_name_td + download_xpath, log=False
                )
            except Exception as e:
                pass

        # 파일 다운로드 및 내역삭제
        self.selenium_scraper.file_download(
            xpath=file_name_td + download_xpath, timeout=timeout
        )
        delete_xpath = '/following-sibling::td[3]//*[text()="내역삭제"]'
        self.selenium_scraper.click_element(xpath=file_name_td + delete_xpath)

        return True

    def upload_xlsx(self, file_path: Path):
        self.selenium_scraper.fetch_page(
            "http://geobigdata.go.kr/portal/analysis/geoCoding.do"
        )
        self.selenium_scraper.send_key(
            '//input[@id="m_file"]', str(file_path), msg="파일 업로드 성공"
        )
        self.selenium_scraper.click_element('//input[@value="칼럼을 선택하세요."]')
        self.selenium_scraper.click_element('//li[text()="REFADDR"]')
        self.selenium_scraper.select_by_option_value(
            '//select[@id="charsetSelect"]', "EUC-KR"
        )
