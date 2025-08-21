import os
import time
import logging
import random
import uuid
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import chromedriver_autoinstaller

logger = logging.getLogger(__name__)

def html_to_image(html_content):
    try:
        # 타임스탬프와 랜덤 문자열로 고유한 파일명 생성
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        random_str = str(uuid.uuid4())[:8]
        unique_id = f"{timestamp}_{random_str}"
        
        # 이미지 저장 디렉토리 설정
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        images_dir = os.path.join(base_dir, "images", "html_table")
        os.makedirs(images_dir, exist_ok=True)
        
        # 임시 HTML 파일 및 출력 이미지 경로 설정
        temp_dir = os.path.join(base_dir, "temp")
        os.makedirs(temp_dir, exist_ok=True)
        temp_html_path = os.path.join(temp_dir, f"temp_{unique_id}.html")
        output_image_path = os.path.join(images_dir, f"table_{unique_id}.png")
        
        # 직업별 인구와 동일한 방식으로 폰트 설정 - 상대경로 사용
        html_with_font = f"""
        <!DOCTYPE html>
        <html lang="ko">
        <head>
            <meta charset="UTF-8">
            <style>
                @font-face {{
                    font-family: 'D2Coding';
                    src: url('../D2Coding.ttf') format('truetype');
                }}
                body {{
                    font-family: 'D2Coding', 'Malgun Gothic', 'Apple Gothic', sans-serif;
                }}
                * {{
                    font-family: 'D2Coding', 'Malgun Gothic', 'Apple Gothic', sans-serif !important;
                }}
            </style>
        </head>
        <body>
            {html_content}
        </body>
        </html>
        """
        
        # 임시 HTML 파일 작성
        with open(temp_html_path, 'w', encoding='utf-8') as f:
            f.write(html_with_font)
        
        # ChromeDriver 설치 확인 및 경로 가져오기
        chromedriver_path = chromedriver_autoinstaller.install()
        
        # Chrome 옵션 설정 (직업별 인구와 동일하게)
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1600,2000')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        # Chrome 드라이버 실행
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # HTML 파일 로드
        file_url = f"file:///{temp_html_path.replace(os.sep, '/')}"
        driver.get(file_url)
        
        # 렌더링될 시간 기다리기
        time.sleep(2)
        
        # 테이블 요소 찾기
        try:
            # 테이블 요소 찾기 시도
            table_element = driver.find_element('css selector', 'table')
            
            # 테이블이 있는 경우 해당 요소만 캡처
            logger.info(f"테이블 요소 찾음: 요소만 캡처합니다.")
            table_element.screenshot(output_image_path)
        except Exception as table_error:
            # 테이블을 찾지 못하면 전체 페이지 캡처
            logger.warning(f"테이블 요소를 찾지 못함: {table_error}, 전체 페이지를 캡처합니다.")
            driver.save_screenshot(output_image_path)
        
        # 브라우저 종료
        driver.quit()
        
        # 임시 파일 삭제
        if os.path.exists(temp_html_path):
            os.remove(temp_html_path)
        
        # 이미지 URL 경로 생성 (상대 경로)
        image_url = f"/images/html_table/{os.path.basename(output_image_path)}"
        
        logger.info(f"HTML 테이블 이미지 생성 성공: {image_url}")
        return {
            "success": True,
            "imageUrl": image_url
        }
    except Exception as e:
        logger.error(f"HTML 테이블 이미지 변환 오류: {e}")
        return {
            "success": False,
            "error": str(e)
        }
