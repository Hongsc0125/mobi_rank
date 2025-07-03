"""
고속 랭킹 서비스

지속적인 페이지 캐시를 활용하여 기존 방식 대비 3-5배 빠른 검색 성능을 제공합니다.
"""

import logging
from service.persistent_ranking_cache import get_ranking_cache
from service.full_data import fetch_all_ranks  # 폴백용

logger = logging.getLogger(__name__)

def fetch_all_ranks_fast(server, character_name):
    """
    지속적인 페이지 캐시를 활용한 고속 랭킹 검색
    
    Args:
        server: 서버명
        character_name: 캐릭터명
        
    Returns:
        dict: 랭킹 검색 결과 (기존 fetch_all_ranks와 동일한 형식)
    """
    try:
        # 캐시된 페이지를 사용한 고속 검색 시도
        cache = get_ranking_cache()
        
        if cache.initialized and cache.running:
            logger.info(f"고속 캐시 검색 시작: 서버={server}, 캐릭터={character_name}")
            result = cache.fast_search(server, character_name)
            
            if result:
                logger.info(f"고속 캐시 검색 성공: {len(result.get('ranks', {}))}개 랭킹 타입 처리")
                return result
            else:
                logger.warning("고속 캐시 검색 결과 없음, 기존 방식으로 폴백")
        else:
            logger.warning("랭킹 캐시가 초기화되지 않음, 기존 방식 사용")
            
    except Exception as e:
        logger.error(f"고속 캐시 검색 중 오류 발생: {e}, 기존 방식으로 폴백")
    
    # 폴백: 기존 방식 사용
    logger.info(f"기존 방식 검색 시작: 서버={server}, 캐릭터={character_name}")
    return fetch_all_ranks(server, character_name)

def is_fast_search_available():
    """고속 검색이 사용 가능한지 확인"""
    try:
        cache = get_ranking_cache()
        return cache.initialized and cache.running
    except:
        return False

def get_cache_status():
    """캐시 상태 정보 반환"""
    try:
        cache = get_ranking_cache()
        return {
            "initialized": cache.initialized,
            "running": cache.running,
            "drivers_count": len(cache.drivers),
            "available_rank_types": list(cache.drivers.keys()),
            "last_servers": dict(cache.last_server)
        }
    except Exception as e:
        return {
            "error": str(e),
            "initialized": False,
            "running": False
        }