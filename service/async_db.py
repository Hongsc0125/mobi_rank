import threading
import logging
from service.db import insert_data

logger = logging.getLogger(__name__)

def async_insert_data(data, server=None, character=None):
    """
    데이터베이스 insert/update를 비동기적으로 실행하는 함수
    """
    def _background_task():
        try:
            result = insert_data(data, server, character)
            logger.info(f"Async DB update completed: {result.get('rows_affected')} rows affected")
        except Exception as e:
            logger.error(f"Error in async DB update: {e}")
    
    # 새 스레드를 생성하여 DB 작업 실행
    thread = threading.Thread(target=_background_task)
    thread.daemon = True
    thread.start()
    
    return {"success": True, "message": "Async DB update started"}
