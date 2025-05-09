from service.full_data import fetch_rank_via_requests, parse_rank_html
from service.db import get_character_data, has_recent_data
from service.async_db import async_insert_data
import logging
from datetime import datetime # Added import
import pytz # Added import

logger = logging.getLogger(__name__)

# Define KST timezone
KST = pytz.timezone('Asia/Seoul') # Added KST timezone

def rank_data(server=None, name=""):
    try:
        # 먼저 캐시에서 데이터 확인
        recent_data = has_recent_data(server, name)
        if recent_data:
            logger.info(f"캐시에서 데이터 검색: {recent_data}")
            return {
                "success": True, 
                "data": recent_data,
                "message": "Retrieved from cache (less than 10 minutes old)",
                "from_cache": True
            }
        
        # 캐시에 없으면 API에서 가져오기
        html_data = fetch_rank_via_requests(server, name)
        # logger.info(f"API에서 데이터 가져오기: {html_data}")
        parsed_data = parse_rank_html(html_data)
        logger.info(f"파싱된 데이터: {parsed_data}")
        
        # 결과가 비어있는지 확인
        if not parsed_data:
            logger.warning(f"API에서 데이터가 비어있음: {html_data}")
            return {
                "success": False,
                "data": None,
                "message": f"캐릭터 '{name}'을(를) 서버 '{server}'에서 찾을 수 없습니다.",
                "from_cache": False
            }
        
        # 요청한 캐릭터 데이터 먼저 찾기 - 대소문자 무시하고 비교
        character_data = None
        for item in parsed_data:
            if (item['server'] == server and 
                item['character'] == name):
                character_data = item
                logger.info(f"캐릭터 '{name}' 데이터 찾음: {character_data}")
                break
        
        # 비동기로 DB 업데이트 시작 (응답을 기다리지 않음)
        now_kst = datetime.now(KST)
        async_insert_data(parsed_data, server, name, retrieved_at_kst=now_kst)
        
        if character_data:
            return {
                "success": True, 
                "data": character_data,
                "message": "Character found in rankings (DB update in progress)",
                "from_cache": False
            }
        else:
            # 데이터는 있지만 특정 캐릭터를 찾지 못한 경우
            return {
                "success": False,
                "data": None,
                "message": f"캐릭터 '{name}'을(를) 서버 '{server}'에서 찾을 수 없습니다.",
                "from_cache": False
            }
    except ValueError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        return {"success": False, "error": "서버 에러: "+str(e)}