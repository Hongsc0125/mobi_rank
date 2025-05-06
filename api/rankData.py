from service.full_data import fetch_rank_via_requests, parse_rank_html
from service.db import get_character_data, has_recent_data
from service.async_db import async_insert_data


def rank_data(server=None, name=""):
    try:
        # 먼저 캐시에서 데이터 확인
        recent_data = has_recent_data(server, name)
        if recent_data:
            return {
                "success": True, 
                "data": recent_data,
                "message": "Retrieved from cache (less than 10 minutes old)",
                "from_cache": True
            }
        
        # 캐시에 없으면 API에서 가져오기
        html_data = fetch_rank_via_requests(server, name)
        parsed_data = parse_rank_html(html_data)
        
        # 요청한 캐릭터 데이터 먼저 찾기
        character_data = None
        for item in parsed_data:
            if item['server'] == server and item['character'] == name:
                character_data = item
                break
        
        # 비동기로 DB 업데이트 시작 (응답을 기다리지 않음)
        async_insert_data(parsed_data, server, name)
        
        if character_data:
            return {
                "success": True, 
                "data": character_data,
                "message": "Character found in rankings (DB update in progress)",
                "from_cache": False
            }
        else:
            # 캐릭터를 찾지 못했으면 모든 데이터 반환
            return {
                "success": True, 
                "data": parsed_data,
                "message": "Character not found in rankings, returning all data (DB update in progress)",
                "from_cache": False
            }
    except ValueError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        return {"success": False, "error": "서버 에러: "+str(e)}