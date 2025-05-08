from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime, timedelta

# #logger = logging.get#logger(__name__)

# 데이터베이스 엔진 생성
engine = create_engine(
    "postgresql://super:Wkwkd119%21%21@207.180.212.248:5444/rank_data",
    pool_pre_ping=True,  # 연결 유효성 검사
    echo=False,  # SQL 쿼리 로깅 비활성화
)

# 세션 팩토리 생성
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """DB 세션을 반환하는 함수"""
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        #logger.error(f"Database error: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def has_recent_data(server, character=None, div=1):
    """
    Check if we have data retrieved within the last 10 minutes
    Returns the data if recent, None otherwise
    """
    db = SessionLocal()
    try:
        time_threshold = datetime.now() - timedelta(minutes=10)
        
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

def insert_data(data, server=None, character=None, div=1):
    # First check if we already have recent data
    recent_data = has_recent_data(server, character, div)
    if recent_data:
        #logger.info("Recent data found, skipping database update")
        return {"success": True, "rows_affected": 0, "data": recent_data, "from_cache": True}
    
    # If no recent data, proceed with insert/update
    db = SessionLocal()
    try:
        # We don't want to delete all existing data anymore
        # db.execute(text("DELETE FROM mabinogi_ranking"))
        
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
                (rank_position, change_amount, change_type, server_name, character_name, class_name, power_value, div)
                VALUES (:rank, :change, :change_type, :server, :character, :class, :power, :div)
                ON CONFLICT (character_name, server_name, div) 
                DO UPDATE SET 
                    rank_position = :rank,
                    change_amount = :change,
                    change_type = :change_type,
                    class_name = :class,
                    power_value = :power,
                    retrieved_at = NOW()
            """)
            
            db.execute(query, {
                'rank': rank_position,
                'change': int(change_value),  # 변환된 change 값 사용
                'change_type': item['change_type'],
                'server': item['server'],
                'character': item['character'],
                'class': item['class'],
                'power': power_value,
                'div': div  # Add div parameter to the query
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
    """Get characters beyond rank 1000 for exploration"""
    db = SessionLocal()
    try:
        time_threshold = datetime.now() - timedelta(minutes=20)
        query = text("""
            SELECT character_name FROM mabinogi_ranking
            WHERE server_name = :server_name 
            AND rank_position > 980
            AND retrieved_at < :time_threshold
            ORDER BY rank_position DESC
        """)
        results = db.execute(query, {'server_name': server_name}).fetchall()
        return results
    except Exception as e:
        # logger.error(f"Error getting 980+ data: {e}")
        return []
    finally:
        db.close()