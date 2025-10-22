import logging
import os
import json
import re
import hashlib
import requests
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from sqlalchemy import text

from service.db_session import KST, KadanSessionLocal

# .env 파일 로드
load_dotenv()

# 로깅 설정
logger = logging.getLogger(__name__)

# 프록시 설정
PROXY_CONFIG = {
    "server": os.getenv("PROXY_SERVER"),
    "username": os.getenv("PROXY_USERNAME"),
    "password": os.getenv("PROXY_PASSWORD")
}

# 패치노트 URL 설정
UPDATE_LIST_URL = "https://mabinogimobile.nexon.com/News/Update"
DETAIL_URL_BASE = "https://mabinogimobile.nexon.com/News/Update/"

# 마지막 페이지 해시값 저장 (경량 체크용)
_last_page_hash = None

def check_page_changed_lightweight():
    """
    프록시 없이 경량으로 패치노트 목록 변경 여부만 체크합니다.
    Playwright 사용하지만 프록시는 미사용하여 비용을 절약합니다.

    Returns:
        bool: 패치노트 목록이 변경되었거나 첫 실행이면 True, 변경 없으면 False
    """
    global _last_page_hash
    try:
        # 프록시 없이 Playwright로 간단히 체크 (프록시 비용 없음)
        logger.info(f"경량 체크: 패치노트 페이지 변경 여부 확인 중 (프록시 미사용)")

        with sync_playwright() as p:
            # 브라우저 실행 (헤드리스, 프록시 없음)
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                ]
            )

            # 컨텍스트 생성 (프록시 없음!)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='ko-KR',
                timezone_id='Asia/Seoul'
                # proxy=PROXY_CONFIG 제거 - 프록시 비용 절약
            )

            # 페이지 생성 및 이동
            page = context.new_page()
            page.goto(UPDATE_LIST_URL, timeout=60000, wait_until="load")

            # 동적 콘텐츠 로딩 대기
            import time
            time.sleep(2)

            # HTML 가져오기
            html_content = page.content()

            # 브라우저 종료
            context.close()
            browser.close()

        # BeautifulSoup으로 파싱
        soup = BeautifulSoup(html_content, "html.parser")

        # 패치노트 제목들만 추출
        titles = []
        for a_tag in soup.select('ul.list li.item a.title'):
            title = a_tag.text.strip()
            if title:
                titles.append(title)

        # 제목 목록의 해시값 계산 (순서 포함)
        titles_str = "|".join(titles)
        current_hash = hashlib.md5(titles_str.encode()).hexdigest()

        logger.info(f"경량 체크: 패치노트 {len(titles)}개 제목 추출 (hash: {current_hash[:8]}...)")

        # 첫 실행
        if _last_page_hash is None:
            _last_page_hash = current_hash
            logger.info(f"경량 체크: 첫 실행 - 해시값 초기화")
            return True

        # 변경 감지
        if current_hash != _last_page_hash:
            logger.info(f"경량 체크: 패치노트 목록 변경 감지! (이전: {_last_page_hash[:8]}... -> 현재: {current_hash[:8]}...)")
            _last_page_hash = current_hash
            return True

        # 변경 없음
        logger.info(f"경량 체크: 변경 없음")
        return False

    except Exception as e:
        logger.error(f"경량 체크 실패: {e}")
        return False

def fetch_patch_note_list():
    """
    Playwright를 사용하여 패치노트 목록을 가져옵니다.

    Returns:
        list: 패치노트 정보 목록 (title, id)
    """
    try:
        with sync_playwright() as p:
            # 브라우저 실행
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                ]
            )

            # 컨텍스트 생성 (프록시 설정 포함)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                locale='ko-KR',
                timezone_id='Asia/Seoul',
                proxy=PROXY_CONFIG
            )

            # 페이지 생성
            page = context.new_page()

            # 패치노트 목록 페이지 이동
            logger.info(f"패치노트 페이지 이동: {UPDATE_LIST_URL}")
            page.goto(UPDATE_LIST_URL, timeout=120000, wait_until="load")  # 2분 타임아웃

            # 동적 콘텐츠 로딩 대기
            import time
            time.sleep(3)  # JavaScript 실행 대기

            # HTML 가져오기
            html_content = page.content()

            # 브라우저 종료
            context.close()
            browser.close()

            # BeautifulSoup으로 파싱
            soup = BeautifulSoup(html_content, "html.parser")

            notes = []
            for a_tag in soup.select('ul.list li.item a.title'):
                title = a_tag.text.strip()
                onclick = a_tag.get("onclick", "")
                match = re.search(r'\((\d+),', onclick)
                if match:
                    thread_id = match.group(1)
                    notes.append({"title": title, "id": thread_id})

            logger.info(f"패치노트 목록 {len(notes)}개 가져오기 성공")
            return notes

    except Exception as e:
        logger.error(f"패치노트 목록 가져오기 실패: {e}")
        return []

def crawl_update_detail(update_id):
    """
    Playwright를 사용하여 패치노트 상세 내용을 크롤링합니다.

    Args:
        update_id (str): 패치노트 ID

    Returns:
        dict: 패치노트 상세 데이터
    """
    url = f"{DETAIL_URL_BASE}{update_id}"
    BASE = "https://mabinogimobile.nexon.com"

    try:
        with sync_playwright() as p:
            # 브라우저 실행
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                ]
            )

            # 컨텍스트 생성 (프록시 설정 포함)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                locale='ko-KR',
                timezone_id='Asia/Seoul',
                proxy=PROXY_CONFIG
            )

            # 페이지 생성
            page = context.new_page()

            # 상세 페이지 이동
            page.goto(url, timeout=120000, wait_until="load")  # 2분 타임아웃

            # 동적 콘텐츠 로딩 대기
            import time
            time.sleep(3)  # JavaScript 실행 대기

            # HTML 가져오기
            html_content = page.content()

            # 브라우저 종료
            context.close()
            browser.close()

            # BeautifulSoup으로 파싱
            soup = BeautifulSoup(html_content, "html.parser")

            # 1) 제목
            title_el = soup.select_one(
                "section.view_header_wrap .title_box .title [data-blockcontent]"
            )
            title = title_el.get_text(strip=True) if title_el else ""

            # 2) 날짜 (예: "2025.05.15 09:40")
            date_el = soup.select_one("section.view_header_wrap .date span")
            date_txt = date_el.get_text(strip=True) if date_el else ""
            # "YYYY.MM.DD hh:mm" → "YYYY-MM-DDThh:mm"
            try:
                dt = datetime.strptime(date_txt, "%Y.%m.%d %H:%M")
                # KST 타임존 적용
                post_date = KST.localize(dt).isoformat()
            except ValueError as e:
                logger.warning(f"날짜 파싱 오류: {e}, 원본 텍스트: {date_txt}")
                post_date = datetime.now(KST).isoformat()

            # 3) 본문 HTML
            content_el = soup.select_one(
                "section.view_body_wrap .content_area .content[data-blockcontent]"
            )
            content_html = content_el.decode_contents().strip() if content_el else ""

            # 4) 첨부 파일 (없으면 빈 리스트)
            attachments = []
            for a in soup.select("div.board-files a, div.attach-file a"):
                href = a.get("href", "")
                full_url = href if href.startswith("http") else BASE + href
                attachments.append({
                    "name": a.get_text(strip=True),
                    "url": full_url
                })

            # 5) 동영상 iframe
            videos = []
            if content_el:
                videos = [
                    iframe.get("src", "")
                    for iframe in content_el.select("iframe")
                    if iframe.get("src")
                ]

            # 현재 시간 추가 (KST)
            scraped_at = datetime.now(KST).isoformat()

            # 사용자가 원하는 데이터 구조
            data = {
                "id": update_id,
                "title": title,
                "post_date": post_date,
                "url": url,
                "content_html": content_html,
                "attachments": attachments,
                "videos": videos,
                "scraped_at": scraped_at
            }

            logger.info(f"패치노트 상세 정보 크롤링 성공: {title}")
            return data

    except Exception as e:
        logger.error(f"패치노트 상세 크롤링 중 오류: {e}")
        return None

def fetch_existing_titles():
    """
    데이터베이스에서 기존 패치노트 제목을 가져옵니다.
    
    Returns:
        set: 기존 패치노트 제목 집합
    """
    try:
        session = KadanSessionLocal()
        result = session.execute(text("SELECT title FROM patch_note_data"))
        titles = {row[0] for row in result}
        logger.info(f"기존 패치노트 {len(titles)}개 제목 가져오기 성공")
        return titles
    except Exception as e:
        logger.error(f"기존 패치노트 제목 조회 중 오류: {e}")
        return set()
    finally:
        session.close()

def save_patch_data(data):
    """
    패치노트 데이터를 데이터베이스에 저장합니다.
    
    Args:
        data (dict): 저장할 패치노트 데이터
        
    Returns:
        bool: 저장 성공 여부
    """
    session = KadanSessionLocal()
    try:
        # 현재 시간(KST) 설정
        current_time = datetime.now(KST)
        
        # 데이터 저장
        query = """
            INSERT INTO patch_note_data (title, post_date, contents_json, id, scraped_at)
            VALUES (:title, :post_date, :contents_json, :id, :scraped_at)
        """
        
        # 사용자가 원하는 형태로 JSON 저장
        # 전체 데이터를 contents_json에 저장
        params = {
            "title": data["title"],
            "post_date": data["post_date"],
            "contents_json": json.dumps(data),  # 전체 데이터를 JSON으로 저장
            "id": data["id"],
            "scraped_at": current_time
        }
        
        session.execute(text(query), params)
        session.commit()
        logger.info(f"패치노트 저장 성공: {data['title']}")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"패치노트 저장 중 오류: {e}")
        return False
    finally:
        session.close()

def send_update_signal():
    """
    패치노트 업데이트 시그널을 보냅니다.
    """
    try:
        # 외부 API에 신호 전송 (GET 요청)
        signal_url = "http://kadanbot.duckdns.org:3000//api/patch_note"
        response = requests.get(signal_url, timeout=10)
        
        if response.status_code == 200:
            logger.info(f"패치노트 업데이트 시그널 전송 성공: {signal_url}")
            return True
        else:
            logger.warning(f"패치노트 업데이트 시그널 전송 실패. 응답 코드: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"패치노트 업데이트 시그널 전송 중 오류: {e}")
        return False

def check_new_patch_notes():
    """
    새로운 패치노트가 있는지 확인하고 DB에 저장합니다.
    
    Returns:
        int: 새로 저장된 패치노트 수
    """
    try:
        # 패치노트 목록 가져오기
        new_notes = fetch_patch_note_list()
        if not new_notes:
            logger.warning("패치노트 목록을 가져오지 못했습니다.")
            return 0
        
        # 기존 패치노트 제목 가져오기
        existing_titles = fetch_existing_titles()
        
        # 새로운 패치노트 저장
        new_count = 0
        for note in new_notes:
            if note["title"] in existing_titles:
                continue
                
            logger.info(f"새로운 패치노트 발견: {note['title']}")
            
            # 상세 정보 크롤링
            data = crawl_update_detail(note["id"])
            if not data:
                logger.warning(f"상세 정보 크롤링 실패: {note['title']}")
                continue
                
            # DB에 저장
            if save_patch_data(data):
                new_count += 1
        
        # 새로운 패치노트가 저장되었을 경우 시그널 전송
        if new_count > 0:
            logger.info(f"새로운 패치노트 {new_count}개 저장 완료, 업데이트 시그널 전송 시도...")
            send_update_signal()
        else:
            pass
            # logger.info("새로운 패치노트가 없어 시그널을 전송하지 않습니다.")
                
        return new_count
    except Exception as e:
        logger.error(f"패치노트 검사 중 오류 발생: {e}")
        return 0
