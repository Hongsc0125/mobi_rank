import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import re
import pytz
from sqlalchemy import text

from service.db_session import KST, KadanSessionLocal

# 로깅 설정
logger = logging.getLogger(__name__)

# 패치노트 URL 설정
UPDATE_LIST_URL = "https://mabinogimobile.nexon.com/News/Update"
DETAIL_URL_BASE = "https://mabinogimobile.nexon.com/News/Update/"

def fetch_patch_note_list():
    """
    패치노트 목록을 가져옵니다.
    
    Returns:
        list: 패치노트 정보 목록 (title, id)
    """
    try:
        resp = requests.get(UPDATE_LIST_URL, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        notes = []
        for a_tag in soup.select('ul.list li.item a.title'):
            title = a_tag.text.strip()
            onclick = a_tag.get("onclick", "")
            match = re.search(r'\((\d+),', onclick)
            if match:
                thread_id = match.group(1)
                notes.append({"title": title, "id": thread_id})
                
        # logger.info(f"패치노트 목록 {len(notes)}개 가져오기 성공")
        return notes
    except Exception as e:
        logger.error(f"패치노트 목록 가져오기 실패: {e}")
        return []

def crawl_update_detail(update_id):
    """
    패치노트 상세 내용을 크롤링합니다.
    
    Args:
        update_id (str): 패치노트 ID
        
    Returns:
        dict: 패치노트 상세 데이터
    """
    url = f"{DETAIL_URL_BASE}{update_id}"
    BASE = "https://mabinogimobile.nexon.com"
    
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
            )
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        
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
