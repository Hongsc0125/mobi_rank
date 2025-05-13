from service.full_data import fetch_all_ranks, parse_rank_html
from service.db import get_character_data, has_recent_data, get_all_ranks_data
from service.async_db import async_insert_all_rank_data
import logging
from service.db_session import KST, get_current_time

logger = logging.getLogger(__name__)

def normalize_rank_data(data):
    """랭킹 데이터 형식을 통일합니다 (캐싱 데이터와 실시간 데이터 간 일관성 유지)"""
    if not data:
        return data
        
    # 1. rankings 객체 정규화
    if "rankings" in data:
        for rank_type, rank_info in data["rankings"].items():
            # change 값이 문자열인 경우 정수로 변환
            if rank_info and "change" in rank_info:
                try:
                    if isinstance(rank_info["change"], str):
                        # "-" 또는 "new"인 경우 0으로 처리
                        if rank_info["change"] == "-" or rank_info["change"].lower() == "new":
                            rank_info["change"] = 0
                        else:
                            # 숫자만 추출
                            import re
                            num_match = re.search(r'\d+', rank_info["change"])
                            if num_match:
                                rank_info["change"] = int(num_match.group(0))
                            else:
                                rank_info["change"] = 0
                except (ValueError, TypeError):
                    rank_info["change"] = 0
                    
            # change_type이 없는 경우 추가
            if rank_info and "change_type" not in rank_info and "change" in rank_info:
                rank_info["change_type"] = "none"  # 기본값
                
    return data

def rank_data(server=None, name=""):
    try:
        # 먼저 캐시에서 데이터 확인 (세 가지 랭킹에 대한 캐시 검색)
        recent_all_ranks = get_all_ranks_data(server, name)
        
        # 캐시가 있으면 형식 통일 후 반환
        if recent_all_ranks and recent_all_ranks.get("rankings"):
            normalized_data = normalize_rank_data(recent_all_ranks)
            logger.info(f"캐시에서 모든 랭킹 데이터 검색 및 정규화: {normalized_data}")
            return {
                "success": True, 
                "data": normalized_data,
                "message": "Retrieved from cache (less than 10 minutes old)",
                "from_cache": True
            }
        
        # 캐시에 없으면 세 가지 랭킹(전투력, 매력, 생활력) 데이터를 동시에 가져옴
        logger.info(f"서버 '{server}'에서 캐릭터 '{name}'의 모든 랭킹 동시 조회 시작")
        all_ranks_data = fetch_all_ranks(server, name)
        logger.info(f"세 가지 랭킹 데이터 조회 완료: {all_ranks_data}")
        
        # 전투력 데이터가 비어있는지 확인
        combat_data = all_ranks_data.get("ranks", {}).get("전투력", {}).get("data", [])
        charm_data = all_ranks_data.get("ranks", {}).get("매력", {}).get("data", [])
        life_data = all_ranks_data.get("ranks", {}).get("생활력", {}).get("data", [])
        
        if not combat_data and not charm_data and not life_data:
            logger.warning(f"모든 랭킹에서 데이터가 비어있음")
            return {
                "success": False,
                "data": None,
                "message": f"캐릭터 '{name}'을(를) 서버 '{server}'에서 찾을 수 없습니다.",
                "from_cache": False
            }
        
        # 비동기로 세 가지 랭킹 데이터 모두 DB에 저장
        logger.info(f"DB에 세 가지 랭킹 데이터 저장 시작")
        async_insert_all_rank_data(all_ranks_data, server, name)
        logger.info(f"DB 업데이트 요청 완료")
        
        
        # 캐릭터 데이터 찾기 (모든 랭킹에서 찾음)
        character_data = {
            "character": name,
            "server": server,
            "retrieved_at": all_ranks_data.get("retrieved_at"),
            "rankings": {
                "전투력": None,
                "매력": None,
                "생활력": None
            }
        }
        
        # 랭킹 데이터 출력을 위한 로깅
        logger.info(f"all_ranks_data: {all_ranks_data}")
        
        # 각 랭킹에서 캐릭터 검색
        for rank_type, rank_data in all_ranks_data.get("ranks", {}).items():
            logger.info(f"{rank_type} 랭킹 데이터: {rank_data}")
            logger.info(f"{rank_type} 랭킹 데이터 항목 수: {len(rank_data.get('data', []))}")
            
            for item in rank_data.get("data", []):
                logger.info(f"검색 중: {item}")
                if item.get('server') == server and item.get('character') == name:
                    logger.info(f"캐릭터 매칭: {item['server']} == {server} and {item['character']} == {name}")
                    character_data["rankings"][rank_type] = item
                    logger.info(f"캐릭터 '{name}'의 {rank_type} 랭킹 데이터 찾음: {item}")
                    break
        
        # 캐릭터 데이터 반환
        return {
            "success": True, 
            "data": character_data,
            "message": "Character found in rankings (DB update in progress)",
            "from_cache": False
        }
    except ValueError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        return {"success": False, "error": "서버 에러: "+str(e)}