import threading
import time
import logging
import traceback
from datetime import datetime, timedelta
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from service.full_data import fetch_rank_via_requests, parse_rank_html
from service.db import engine, SessionLocal

logger = logging.getLogger(__name__)

def get_outdated_characters():
    """Get list of characters that need updating (older than 10 minutes)"""
    db = SessionLocal()
    try:
        time_threshold = datetime.now() - timedelta(minutes=10)
        query = text("""
            SELECT server_name, character_name 
            FROM mabinogi_ranking
            WHERE retrieved_at < :threshold
            LIMIT 100
        """)
        
        result = db.execute(query, {'threshold': time_threshold}).fetchall()
        return [{'server': row[0], 'character': row[1]} for row in result]
    except Exception as e:
        logger.error(f"Error getting outdated characters: {e}")
        return []
    finally:
        db.close()

def update_character_data(server, character):
    """Update data for a specific character"""
    try:
        html_data = fetch_rank_via_requests(server, character)
        parsed_data = parse_rank_html(html_data)
        
        # Find the character in parsed data
        character_data = None
        for item in parsed_data:
            if item['server'] == server and item['character'] == character:
                character_data = item
                break
        
        if not character_data:
            logger.warning(f"Character {character} on {server} not found in rankings")
            return False
            
        # Update database
        db = SessionLocal()
        try:
            # 랭크 위치와 전투력 변환
            rank_position = int(character_data['rank'].replace(',', '').replace('위', ''))
            power_value = int(character_data['power'].replace(',', ''))
            
            # change 값이 '-'인 경우 0으로 처리
            change_value = character_data['change']
            if change_value == '-':
                change_value = '0'
            
            query = text("""
                UPDATE mabinogi_ranking
                SET rank_position = :rank,
                    change_amount = :change,
                    change_type = :change_type,
                    class_name = :class,
                    power_value = :power,
                    retrieved_at = NOW()
                WHERE server_name = :server AND character_name = :character
            """)
            
            db.execute(query, {
                'rank': rank_position,
                'change': int(change_value),  # 변환된 change 값 사용
                'change_type': character_data['change_type'],
                'server': server,
                'character': character,
                'class': character_data['class'],
                'power': power_value
            })
            db.commit()
            logger.info(f"Updated data for {character} on {server}")
            return True
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating {character} on {server}: {e}")
            return False
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in update_character_data: {e}")
        logger.error(traceback.format_exc())
        return False

def background_update_task():
    """Background task to update outdated character data"""
    while True:
        try:
            characters = get_outdated_characters()
            if characters:
                logger.info(f"Found {len(characters)} characters to update")
                
                for char in characters:
                    update_character_data(char['server'], char['character'])
                    # Small delay to avoid overwhelming the API
                    time.sleep(1)
            
            # Sleep for at least 30 seconds between update cycles
            time.sleep(30)
            
        except Exception as e:
            logger.error(f"Error in background update task: {e}")
            logger.error(traceback.format_exc())
            # Sleep longer after errors
            time.sleep(60)

def start_background_tasks():
    """Start all background tasks in separate threads"""
    # Start character update thread
    update_thread = threading.Thread(target=background_update_task, daemon=True)
    update_thread.name = "character-update-thread"
    update_thread.start()
    logger.info("Started background character update thread")
    
    return update_thread
