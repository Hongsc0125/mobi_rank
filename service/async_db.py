import threading
import logging
from service.db import insert_data
from service.db_session import KST, get_current_time

# 로거 설정
logger = logging.getLogger(__name__)

def async_insert_data(data, server=None, character=None, retrieved_at_kst=None): # Added retrieved_at_kst
    """
    데이터베이스 insert/update를 비동기적으로 실행하는 함수
    """
    def _background_task():
        try:
            result = insert_data(data, server, character, retrieved_at_kst=retrieved_at_kst)
            logger.info(f"비동기 DB 업데이트 완료: {result.get('rows_affected')}행이 변경됨")
        except Exception as e:
            logger.error(f"비동기 DB 업데이트 오류: {e}")
            print(f"Error in async DB update: {e}")
    
    # 새 스레드를 생성하여 DB 작업 실행
    thread = threading.Thread(target=_background_task)
    thread.daemon = True
    thread.start()
    
    return {"success": True, "message": "Async DB update started"}
