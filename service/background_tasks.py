import threading
import time
import logging
import traceback
from datetime import datetime, timedelta
from sqlalchemy import text
from service.full_data import fetch_rank_via_requests, parse_rank_html
from service.db_session import SessionLocal, KST, get_current_time
from service.population import get_all_server_populations, generate_population_graph
from service.population_statistics import update_population_statistics

# #logger = logging.get#logger(__name__)

def get_outdated_characters():
    db = SessionLocal()
    try:
        time_threshold = get_current_time() - timedelta(minutes=10)
        query = text("""
            SELECT server_name, character_name 
            FROM mabinogi_ranking
            WHERE retrieved_at < :threshold
            LIMIT 100
        """)
        
        result = db.execute(query, {'threshold': time_threshold}).fetchall()
        return [{'server': row[0], 'character': row[1]} for row in result]
    except Exception as e:
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
            #logger.warning(f"Character {character} on {server} not found in rankings")
            return False
            
        # Update database
        db = SessionLocal()
        try:
            # 랭크 위치와 전투력 변환
            rank_position = int(character_data['rank'].replace(',', '').replace('위', ''))
            power_value = int(character_data['power'].replace(',', ''))
            
            # change 값 처리
            change_value = character_data['change']
            if change_value == '-':
                change_value = '0'
                
            # change_type이 'down'인 경우 음수로 변환 (랭킹이 내려간 경우)
            change_value_int = int(change_value.replace(',', ''))
            if character_data['change_type'] == 'down':
                change_value_int = -change_value_int  # 내려간 경우 음수로 표현
            
            current_time_for_db = get_current_time()
            query = text("""
                UPDATE mabinogi_ranking
                SET rank_position = :rank,
                    change_amount = :change,
                    change_type = :change_type,
                    class_name = :class,
                    power_value = :power,
                    retrieved_at = :retrieved_at_val
                WHERE server_name = :server AND character_name = :character
            """)
            
            db.execute(query, {
                'rank': rank_position,
                'change': change_value_int,  # 음수/양수로 변환된 change 값 사용
                'change_type': character_data['change_type'],
                'server': server,
                'character': character,
                'class': character_data['class'],
                'power': power_value,
                'retrieved_at_val': current_time_for_db
            })
            db.commit()
            #logger.info(f"Updated data for {character} on {server}")
            return True
        except Exception as e:
            db.rollback()
            #logger.error(f"Error updating {character} on {server}: {e}")
            return False
        finally:
            db.close()
            
    except Exception as e:
        #logger.error(f"Error in update_character_data: {e}")
        #logger.error(traceback.format_exc())
        return False

def background_update_task():
    """Background task to update outdated character data"""
    while True:
        try:
            characters = get_outdated_characters()
            if characters:
                #logger.info(f"Found {len(characters)} characters to update")
                
                for char in characters:
                    update_character_data(char['server'], char['character'])
                    # Small delay to avoid overwhelming the API
                    time.sleep(1)
            
            # Sleep for at least 30 seconds between update cycles
            time.sleep(30)
            
        except Exception as e:
            #logger.error(f"Error in background update task: {e}")
            #logger.error(traceback.format_exc())
            # Sleep longer after errors
            time.sleep(60)

def update_population_data():
    """최신 인구수 데이터를 업데이트하는 백그라운드 작업
    1시간 이내에 이미 업데이트한 이력이 있으면 갱신을 스킵합니다.
    """
    from service.population import _population_cache
    
    while True:
        try:
            # 현재 시간(KST) 가져오기
            current_time = get_current_time()
            
            # 마지막 업데이트 시간 확인
            last_updated = _population_cache.get("last_updated")
            
            # 1시간 이내 업데이트 이력이 있는지 확인
            if last_updated and (current_time - last_updated).total_seconds() < 3600:
                one_hour_ago = (current_time - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S KST')
                print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 인구수 데이터가 최근({one_hour_ago} 이후) 업데이트되었으므로 스킵합니다.")
                # 다음 업데이트 시간까지 대기
                remaining_time = 3600 - (current_time - last_updated).total_seconds()
                time.sleep(max(60, remaining_time))  # 최소 1분 이상 대기
                continue
            
            # 1시간 이상 지났거나 처음 실행이면 데이터 갱신
            print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 인구수 데이터 업데이트 시작...")
            population_data = get_all_server_populations()
            
            if population_data:
                # 현재 시간(KST)
                timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S KST')
                
                # 그래프 생성
                image_filename = generate_population_graph(population_data)
                image_url = f"/images/{image_filename}" if image_filename else None
                
                # 캐시 업데이트
                _population_cache["data"] = population_data
                _population_cache["timestamp"] = timestamp
                _population_cache["imageUrl"] = image_url
                _population_cache["cache_time"] = current_time
                _population_cache["last_updated"] = current_time
                
                print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 인구수 데이터 업데이트 완료")
                
                # 인구수 통계 수집 및 DB 저장
                # update_population_statistics()
            
            # 1시간 대기
            time.sleep(3600)  # 1시간 = 3600초
            
        except Exception as e:
            print(f"인구수 데이터 업데이트 오류: {e}")
            traceback.print_exc()
            # 오류 발생 시 10분 후 재시도
            time.sleep(600)


def update_class_population_data():
    """직업별 인구 통계 데이터를 업데이트하는 백그라운드 작업
    1시간 이내에 이미 업데이트한 이력이 있으면 갱신을 스킵합니다.
    """
    from service.population_statistics import _class_population_cache, update_class_population_cache
    
    while True:
        try:
            # 현재 시간(KST) 가져오기
            current_time = get_current_time()
            
            # 마지막 업데이트 시간 확인
            last_updated = _class_population_cache.get("last_updated")
            
            # 1시간 이내 업데이트 이력이 있는지 확인
            if last_updated and (current_time - last_updated).total_seconds() < 3600:
                one_hour_ago = (current_time - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S KST')
                print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 직업별 인구 통계가 최근({one_hour_ago} 이후) 업데이트되었으므로 스킵합니다.")
                # 다음 업데이트 시간까지 대기
                remaining_time = 3600 - (current_time - last_updated).total_seconds()
                time.sleep(max(60, remaining_time))  # 최소 1분 이상 대기
                continue
            
            # 1시간 이상 지났거나 처음 실행이면 데이터 갱신
            print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 직업별 인구 통계 업데이트 시작...")
            
            # 직업별 인구 통계 생성 및 캐시 업데이트
            result = update_class_population_cache()
            
            if result:
                print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 직업별 인구 통계 업데이트 완료")
            else:
                print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 직업별 인구 통계 업데이트 실패")
            
            # 1시간 대기
            time.sleep(3600)  # 1시간 = 3600초
            
        except Exception as e:
            print(f"직업별 인구 통계 업데이트 오류: {e}")
            traceback.print_exc()
            # 오류 발생 시 10분 후 재시도
            time.sleep(600)

def update_population_statistics_task():
    """인구수 통계 데이터를 매일 00시(한국 시간)에 업데이트하는 백그라운드 작업
    매일 자정 시간에 1회만 실행합니다.
    """
    # 마지막 업데이트 시간 추적 (날짜 기준)
    last_update_date = None
    
    while True:
        try:
            # 현재 시간(KST)
            current_time = get_current_time()
            current_date = current_time.date()
            
            # 오늘 이미 업데이트했는지 확인
            if last_update_date == current_date:
                print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 오늘({current_date}) 이미 인구 통계를 업데이트했으므로 스킵합니다.")
                time.sleep(1800)  # 30분 후 다시 확인
                continue
            
            # 00:00~00:10 사이에만 실행 (공서버는 결군 정각에 정확하게 실행되지 않을 수 있음)
            if current_time.hour == 0 and current_time.minute < 10:
                print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 인구수 통계 데이터 업데이트 시작...")
                result = update_population_statistics()
                
                if result:
                    print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 인구수 통계 데이터 업데이트 완료")
                    # 오늘 업데이트 완료 표시
                    last_update_date = current_date
                    
                    # 다음 시간대 까지 충분히 잠 (현재가 00시대면 다음 00시까지 대기)
                    time.sleep(3600)  # 1시간 대기
                else:
                    print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 인구수 통계 데이터 업데이트 실패")
                    time.sleep(300)  # 5분 후 재시도
            else:
                # 다음 00시 가까워지면 더 확인 빈도 높임
                minutes_to_midnight = ((23 - current_time.hour) * 60) + (60 - current_time.minute)
                
                if minutes_to_midnight < 30:  # 00시 30분 전이면
                    time.sleep(300)  # 5분마다 확인
                else:
                    time.sleep(1800)  # 그 외에는 30분마다 확인
            
        except Exception as e:
            print(f"인구수 통계 업데이트 오류: {e}")
            traceback.print_exc()
            # 오류 발생 시 15분 후 재시도
            time.sleep(900)
    """Start all background tasks in separate threads"""
    # 크롬 드라이버 풀 초기화
    from service.driver_pool import get_driver_pool
    driver_pool = get_driver_pool()  # 3개의 크롬 드라이버를 상시 유지하는 풀 초기화
    print(f"크롬 드라이버 풀 초기화 완료 (최대 3개 드라이버 유지)")
    
    # 캐릭터 업데이트 스레드 시작
    update_thread = threading.Thread(target=background_update_task, daemon=True)
    update_thread.name = "character-update-thread"
    update_thread.start()
    #logger.info("Started background character update thread")
    
    # 인구수 업데이트 스레드 시작 (1개 쓰레드만 배정)
    population_thread = threading.Thread(target=update_population_data, daemon=True)
    population_thread.name = "population-update-thread"
    population_thread.start()
    print("인구수 업데이트 백그라운드 쓰레드 시작됨 (1개 쓰레드)")
    
    # 직업별 인구 통계 업데이트 스레드 시작
    class_population_thread = threading.Thread(target=update_class_population_data, daemon=True)
    class_population_thread.name = "class-population-update-thread"
    class_population_thread.start()
    print("직업별 인구 통계 업데이트 백그라운드 쓰레드 시작됨")
    
    # 인구수 통계 업데이트 스레드 시작
    stats_thread = threading.Thread(target=update_population_statistics_task, daemon=True)
    stats_thread.name = "population-statistics-thread"
    stats_thread.start()
    print("인구수 통계 업데이트 백그라운드 쓰레드 시작됨")
    
    return [update_thread, population_thread, stats_thread]
