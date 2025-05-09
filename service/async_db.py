import threading
import logging
from service.db import insert_data
from datetime import datetime # Added import
import pytz # Added import

# Define KST timezone
KST = pytz.timezone('Asia/Seoul') # Added KST timezone

# logger = logging.getLogger(__name__)

def async_insert_data(data, server=None, character=None, retrieved_at_kst=None): # Added retrieved_at_kst
    """
    데이터베이스 insert/update를 비동기적으로 실행하는 함수
    """
    def _background_task():
        try:
            # Pass retrieved_at_kst to insert_data. 
            # If None, insert_data in db.py will generate its own KST timestamp.
            result = insert_data(data, server, character, retrieved_at_kst=retrieved_at_kst)
            # logger.info(f"Async DB update completed: {result.get('rows_affected')} rows affected")
        except Exception as e:
            # logger.error(f"Error in async DB update: {e}")
            print(f"Error in async DB update: {e}")
    
    # 새 스레드를 생성하여 DB 작업 실행
    thread = threading.Thread(target=_background_task)
    thread.daemon = True
    thread.start()
    
    return {"success": True, "message": "Async DB update started"}
