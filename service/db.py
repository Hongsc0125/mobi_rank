from sqlalchemy import text
from datetime import datetime, timedelta
import pytz
from sqlalchemy import Text
import logging

# 로거 설정
logger = logging.getLogger(__name__)

# 중앙화된 DB 세션 관리 모듈 import
from .db_session import SessionLocal, get_current_time, KST

def has_recent_data(server, character=None, div=1):
    """
    Check if we have data retrieved within the last 10 minutes
    Returns the data if recent, None otherwise
    """
    db = SessionLocal()
    try:
        time_threshold = get_current_time() - timedelta(minutes=10) # Use KST
        
        # Build query based on whether we're looking for a specific character
        if character:
            query = text("""
                SELECT * FROM mabinogi_ranking
                WHERE server_name = :server 
                AND character_name = :character
                AND div = :div
                AND retrieved_at > :threshold
                LIMIT 1
            """)
            params = {'server': server, 'character': character, 'div': div, 'threshold': time_threshold}
        else:
            query = text("""
                SELECT COUNT(*) FROM mabinogi_ranking
                WHERE server_name = :server 
                AND div = :div
                AND retrieved_at > :threshold
            """)
            params = {'server': server, 'div': div, 'threshold': time_threshold}
            
            # Check if we have a minimum number of records (e.g., 20)
            count = db.execute(query, params).scalar()
            if count < 20:  # Require at least 20 records to consider data "fresh"
                return None
                
            # If we have enough recent records, get all matching data
            query = text("""
                SELECT * FROM mabinogi_ranking
                WHERE server_name = :server 
                AND div = :div
                AND retrieved_at > :threshold
            """)
        
        result = db.execute(query, params).fetchall()
        
        if not result:
            return None
            
        # For character-specific query, return just that row
        if character and len(result) > 0:
            columns = ['id', 'rank_position', 'change_amount', 'change_type', 
                      'server_name', 'character_name', 'class_name', 'power_value', 'retrieved_at', 'div']
            return {columns[i]: value for i, value in enumerate(result[0])}
        
        # For server query, return all matching rows as a list
        if not character:
            columns = ['id', 'rank_position', 'change_amount', 'change_type', 
                      'server_name', 'character_name', 'class_name', 'power_value', 'retrieved_at', 'div']
            return [{columns[i]: value for i, value in enumerate(row)} for row in result]
            
        return None
    finally:
        db.close()

def insert_data(data, server=None, character=None, div=1, retrieved_at_kst=None): 
    # logger.info(f"retrieved_at_kst: {retrieved_at_kst}")
    recent_data = has_recent_data(server, character, div)
    if recent_data:
        #logger.info("Recent data found, skipping database update")
        return {"success": True, "rows_affected": 0, "data": recent_data, "from_cache": True}
    
    # If no recent data, proceed with insert/update
    db = SessionLocal()
    try:
        # We don't want to delete all existing data anymore
        # db.execute(text("DELETE FROM mabinogi_ranking"))

        # Ensure KST timezone is used for timestamp
        current_time_for_db = retrieved_at_kst if retrieved_at_kst else get_current_time()
        
        # 타임존 확인 및 설정
        if current_time_for_db and not current_time_for_db.tzinfo:
            current_time_for_db = KST.localize(current_time_for_db)
            
        # logger.info(f"사용할 타임스탬프: {current_time_for_db} (타입: {type(current_time_for_db)})")
        
        for item in data:
            # Convert comma-separated strings to numbers
            rank_position = int(item['rank'].replace(',', '').replace('위', ''))
            power_value = int(item['power'].replace(',', ''))
            
            # change 값이 '-'인 경우 0으로 처리
            change_value = item['change']
            if change_value == '-':
                change_value = '0'
            
            # Insert or update record with div parameter
            query = text("""
                INSERT INTO mabinogi_ranking 
                (rank_position, change_amount, change_type, server_name, character_name, class_name, power_value, div, retrieved_at)
                VALUES (:rank, :change, :change_type, :server, :character, :class, :power, :div, :retrieved_at_val AT TIME ZONE 'Asia/Seoul')
                ON CONFLICT (character_name, server_name, div) 
                DO UPDATE SET 
                    rank_position = :rank,
                    change_amount = :change,
                    change_type = :change_type,
                    class_name = :class,
                    power_value = :power,
                    retrieved_at = :retrieved_at_val AT TIME ZONE 'Asia/Seoul'
            """)
            
            db.execute(query, {
                'rank': rank_position,
                'change': int(change_value),  # 변환된 change 값 사용
                'change_type': item['change_type'],
                'server': item['server'],
                'character': item['character'],
                'class': item['class'],
                'power': power_value,
                'div': div,  # Add div parameter to the query
                'retrieved_at_val': current_time_for_db # Use the KST timestamp
            })
        
        db.commit()
        
        # If character specified, get and return that character's data
        if character and server:
            result = get_character_data(server, character, div)
            return {"success": True, "rows_affected": len(data), "data": result, "from_cache": False}
        
        return {"success": True, "rows_affected": len(data), "from_cache": False}
    except Exception as e:
        db.rollback()
        #logger.error(f"Error inserting data: {e}")
        raise
    finally:
        db.close()

def get_character_data(server, character, div=1):
    db = SessionLocal()
    try:
        query = text("""
            SELECT * FROM mabinogi_ranking
            WHERE server_name = :server AND character_name = :character AND div = :div
        """)
        result = db.execute(query, {'server': server, 'character': character, 'div': div}).fetchone()
        if result:
            # Convert DB row to dictionary
            columns = ['id', 'rank_position', 'change_amount', 'change_type', 
                      'server_name', 'character_name', 'class_name', 'power_value', 'retrieved_at', 'div']
            return {columns[i]: value for i, value in enumerate(result)}
        return None
    finally:
        db.close()


def get_980_data(server_name):
    """Get characters beyond rank 980 for exploration"""
    db = SessionLocal()
    try:
        time_threshold = get_current_time() - timedelta(minutes=20) # Use KST
        query = text("""
            SELECT character_name FROM mabinogi_ranking
            WHERE server_name = :server_name 
            AND rank_position > 980
            AND retrieved_at < :time_threshold
            ORDER BY rank_position DESC
        """)
        params = {'server_name': server_name, 'time_threshold': time_threshold}
        
        # Add this log to check the parameters
        logging.info(f"Executing query with params: {params}")
        
        results = db.execute(query, params).fetchall()
        return results
    except Exception as e:
        # logger.error(f"Error getting 980+ data: {e}")
        return []
    finally:
        db.close()