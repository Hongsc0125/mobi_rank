import threading
import logging
from service.db import insert_data
from service.db_session import KST, get_current_time
import pytz
from datetime import datetime

# 로거 설정
logger = logging.getLogger(__name__)

def async_insert_data(data, server=None, character=None, div=1, retrieved_at_kst=None):
    """
    데이터베이스 insert/update를 비동기적으로 실행하는 함수
    """
    def _background_task():
        try:
            # 문자열이면 datetime으로 변환
            processed_retrieved_at = retrieved_at_kst
            if isinstance(retrieved_at_kst, str):
                try:
                    # 문자열을 datetime으로 변환
                    dt = datetime.strptime(retrieved_at_kst, "%Y-%m-%d %H:%M:%S")
                    # KST 적용
                    processed_retrieved_at = pytz.timezone('Asia/Seoul').localize(dt)
                    logger.info(f"문자열 날짜를 datetime으로 변환: {processed_retrieved_at}")
                except Exception as e:
                    # 변환 오류 발생 시 현재 시간 사용
                    logger.warning(f"날짜 변환 오류: {e}, 현재 시간 사용")
                    processed_retrieved_at = get_current_time()
            elif processed_retrieved_at is None:
                processed_retrieved_at = get_current_time()
                
            result = insert_data(data, server, character, div=div, retrieved_at_kst=processed_retrieved_at)
            # logger.info(f"비동기 DB 업데이트 완료: {result.get('rows_affected')}행이 변경됨")
        except Exception as e:
            logger.error(f"비동기 DB 업데이트 오류: {e}")
            print(f"Error in async DB update: {e}")
    
    # 새 스레드를 생성하여 DB 작업 실행
    thread = threading.Thread(target=_background_task)
    thread.daemon = True
    thread.start()
    
    return {"success": True, "message": "Async DB update started"}


def async_insert_all_rank_data(all_ranks_data, server=None, character=None):
    """
    세 가지 랭킹(전투력, 매력, 생활력) 데이터를 비동기적으로 DB에 저장
    all_ranks_data는 fetch_all_ranks 함수의 반환값 형식에 맞추어야 함
    """
    # 랭킹 타입에 따른 div 매핑
    rank_type_to_div = {
        "전투력": 1,
        "매력": 2,
        "생활력": 3
    }
    
    # retrieved_at 값 가져오기 (모든 랭킹에서 동일)
    retrieved_at_kst = all_ranks_data.get("retrieved_at")
    processed_retrieved_at = None
    
    # 문자열 형태의 시간을 datetime으로 변환
    if isinstance(retrieved_at_kst, str):
        try:
            # 문자열을 datetime으로 변환
            dt = datetime.strptime(retrieved_at_kst, "%Y-%m-%d %H:%M:%S")
            # KST 적용
            processed_retrieved_at = pytz.timezone('Asia/Seoul').localize(dt)
        except Exception as e:
            logger.warning(f"all_ranks_data 날짜 변환 오류: {e}, 현재 시간 사용")
            processed_retrieved_at = get_current_time()
    elif retrieved_at_kst is not None:
        # 이미 datetime 객체라면 그대로 사용
        processed_retrieved_at = retrieved_at_kst
    else:
        # retrieved_at이 없는 경우 현재 시간 사용
        processed_retrieved_at = get_current_time()
    
    # 타임존 확인 및 설정 - KST가 아니면 적용
    if processed_retrieved_at and not processed_retrieved_at.tzinfo or processed_retrieved_at.tzinfo.zone != 'Asia/Seoul':
        if not processed_retrieved_at.tzinfo:
            processed_retrieved_at = KST.localize(processed_retrieved_at)
        else:
            processed_retrieved_at = processed_retrieved_at.astimezone(KST)
        
    # 각 랭킹 데이터에 대해 비동기 DB 업데이트 실행
    for rank_type, rank_data in all_ranks_data.get("ranks", {}).items():
        if rank_type in rank_type_to_div and rank_data.get("data"):
            div = rank_type_to_div[rank_type]
            data = rank_data.get("data", [])
            if data:  # 데이터가 있는 경우만 DB 업데이트 실행
                async_insert_data(data, server, character, div=div, retrieved_at_kst=processed_retrieved_at)
                # logger.info(f"{rank_type} 랭킹 데이터 DB 업데이트 요청됨 (div={div}, 저장시간: {processed_retrieved_at})")
        else:
            logger.info(f"{rank_type} 랭킹 데이터 없음")
    
    
    return {"success": True, "message": "All rank types DB updates started"}
