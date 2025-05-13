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
            
            # change 값 처리
            change_value = item['change']
            if change_value == '-':
                change_value = '0'
            
            # change_type이 'down'인 경우 음수로 변환 (랭킹이 내려간 경우)
            change_value_int = int(change_value.replace(',', ''))
            if item['change_type'] == 'down':
                change_value_int = -change_value_int  # 내려간 경우 음수로 표현
            
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
                'change': change_value_int,  # 음수/양수로 변환된 change 값 사용
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


def get_all_ranks_data(server, character):
    """
    캐릭터의 세 가지 랭킹(전투력, 매력, 생활력) 데이터를 캐시에서 조회
    최근 10분 이내의 데이터만 반환, 없으면 None 반환
    """
    if not server or not character:
        return None
    
    # 랭킹 타입매핑
    div_to_rank_type = {
        1: "전투력",
        2: "매력",
        3: "생활력"
    }
    
    # 현재 시간(한국 시간 기준)
    now_kst = get_current_time()
    
    # 각 랭킹 타입별로 캐시데이터 조회
    ranks_data = {}
    have_data = False
    
    for div, rank_type in div_to_rank_type.items():
        data = has_recent_data(server, character, div)
        if data:
            have_data = True
            ranks_data[rank_type] = {
                "type": rank_type,
                "data": [data],  # DB에서 가져온 경우 단일 레코드이민로 리스트로 만듦
                "retrieved_at": data.get("retrieved_at").strftime("%Y-%m-%d %H:%M:%S") if data.get("retrieved_at") else now_kst.strftime("%Y-%m-%d %H:%M:%S")
            }
        else:
            ranks_data[rank_type] = {
                "type": rank_type,
                "data": [],
                "retrieved_at": now_kst.strftime("%Y-%m-%d %H:%M:%S")
            }
    
    # 적어도 하나의 랭킹 데이터가 있으면 캐시 데이터 반환
    if have_data:
        character_data = {
            "character": character,
            "server": server,
            "retrieved_at": now_kst.strftime("%Y-%m-%d %H:%M:%S"),
            "rankings": ranks_data
        }
        return character_data
    
    # 없으면 None 반환
    return None

def get_980_data(server_name):
    """Get characters beyond rank 980 for exploration"""
    db = SessionLocal()
    try:
        query = text("""
            SELECT server_name, character_name, MAX(retrieved_at) as last_seen
            FROM mabinogi_ranking
            WHERE server_name = :server
            AND rank_value > 980
            GROUP BY server_name, character_name
            ORDER BY server_name, last_seen DESC
            LIMIT 10000
        """)
        
        rows = db.execute(query, {'server': server_name}).fetchall()
        
        # Convert to list of dicts
        result = []
        for row in rows:
            result.append({
                'server_name': row[0],
                'character_name': row[1],
                'last_seen': row[2].strftime('%Y-%m-%d %H:%M:%S') if row[2] else 'Never'
            })
            
        return result
    finally:
        db.close()

def delete_character_data(server, character, div=None):
    """
    DB에서 캐릭터 데이터를 삭제합니다.
    찾을 수 없는 캐릭터를 DB에서 완전히 제거하기 위해 사용됩니다.
    
    Args:
        server: 서버 이름
        character: 캐릭터 이름
        div: 랭킹 구분 (None인 경우 모든 div 삭제)
        
    Returns:
        int: 삭제된 레코드 수
    """
    db = SessionLocal()
    try:
        # 현재 KST 시간 기록 (삭제 시점)
        current_time = get_current_time()
        
        # div 지정 여부에 따라 쿼리 구성
        if div is not None:
            query = text("""
                DELETE FROM mabinogi_ranking
                WHERE server_name = :server
                AND character_name = :character
                AND div = :div
                RETURNING 1
            """)
            params = {'server': server, 'character': character, 'div': div}
        else:
            # 모든 div 데이터 삭제
            query = text("""
                DELETE FROM mabinogi_ranking
                WHERE server_name = :server 
                AND character_name = :character
                RETURNING 1
            """)
            params = {'server': server, 'character': character}
            
        # 쿼리 실행 및 삭제된 레코드 수 카운트
        result = db.execute(query, params)
        deleted_count = len(result.fetchall())
        
        # 변경사항 커밋
        db.commit()
        
        # 로그 기록
        if deleted_count > 0:
            logger.info(f"삭제 완료: 서버={server}, 캐릭터={character}, DIV={div if div else 'ALL'}, 삭제 레코드={deleted_count}개, 시간={current_time}")
        
        return deleted_count
    
    except Exception as e:
        db.rollback()  # 오류 발생 시 롤백
        logger.error(f"캐릭터 삭제 중 오류 발생: {e}")
        raise
    
    finally:
        db.close()