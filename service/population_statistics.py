import logging
import os
import concurrent.futures
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import text
from service.db_session import SessionLocal, get_current_time, KST
from service.population import get_all_server_populations

# 로거 설정
logger = logging.getLogger(__name__)

# 서버 목록
SERVERS = ["데이안", "아이라", "던컨", "알리사", "메이븐", "라사", "칼릭스"]

# 랭킹 타입 (div 값에 따른 분류)
RANK_TYPES = {
    1: "전투력",
    2: "매력",
    3: "생활력"
}

def execute_sql_file(file_path):
    """SQL 파일을 실행하여 테이블을 생성합니다"""
    db = SessionLocal()
    try:
        with open(file_path, 'r', encoding='utf-8') as sql_file:
            sql_content = sql_file.read()
            statements = sql_content.split(';')
            
            for statement in statements:
                if statement.strip():
                    db.execute(text(statement))
            
            db.commit()
            logger.info(f"SQL 파일 {file_path} 실행 완료")
            return True
    except Exception as e:
        db.rollback()
        logger.error(f"SQL 파일 실행 중 오류 발생: {e}")
        return False
    finally:
        db.close()


def initialize_database():
    """테이블이 존재하지 않으면 생성합니다"""
    sql_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                "sql", "population_statistics.sql")
    return execute_sql_file(sql_file_path)


def save_server_population(server_data):
    """
    서버별 인구수 데이터를 DB에 저장합니다.
    
    Args:
        server_data (list): 서버별 인구수 정보 리스트
    """
    if not server_data:
        return False
    
    db = SessionLocal()
    current_time = get_current_time()
    
    try:
        for item in server_data:
            query = text("""
                INSERT INTO server_population_stats
                (server_name, population, retrieved_at) 
                VALUES (:server_name, :population, :retrieved_at)
            """)
            
            db.execute(query, {
                'server_name': item['server_name'],
                'population': item['population'],
                'retrieved_at': current_time
            })
        
        db.commit()
        logger.info(f"서버별 인구수 데이터 저장 완료: {len(server_data)}개 서버")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"서버별 인구수 데이터 저장 중 오류 발생: {e}")
        return False
    finally:
        db.close()


def collect_class_data_all_servers(div=1):
    """
    모든 서버의 직업별 인구수 데이터를 수집합니다.
    
    Args:
        div (int): 랭킹 타입 (1: 전투력, 2: 매력, 3: 생활력)
    
    Returns:
        list: 모든 서버의 직업별 데이터
    """
    all_data = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_server = {
            executor.submit(fetch_class_data_for_server, server, div): server
            for server in SERVERS
        }
        
        for future in concurrent.futures.as_completed(future_to_server):
            server = future_to_server[future]
            try:
                data = future.result()
                if data:
                    all_data.extend(data)
                    logger.info(f"{server} 서버 {RANK_TYPES.get(div, '전투력')} 랭킹 직업별 데이터 수집 완료: {len(data)}개 직업")
            except Exception as e:
                logger.error(f"{server} 서버 직업별 데이터 처리 중 오류: {e}")
    
    return all_data

def save_class_population_data(class_data):
    """
    직업별 인구수 데이터를 DB에 저장합니다.
    
    Args:
        class_data (list): 직업별 인구수 정보 리스트
    """
    if not class_data:
        return False
    
    db = SessionLocal()
    current_time = get_current_time()
    
    try:
        for item in class_data:
            query = text("""
                INSERT INTO class_population_stats
                (server_name, class_name, character_count, percentage, average_power, retrieved_at) 
                VALUES (:server_name, :class_name, :character_count, :percentage, :average_power, :retrieved_at)
            """)
            
            db.execute(query, {
                'server_name': item['server_name'],
                'class_name': item['class_name'],
                'character_count': item['character_count'],
                'percentage': item['percentage'],
                'average_power': item['average_power'],
                'retrieved_at': current_time
            })
        
        db.commit()
        logger.info(f"{RANK_TYPES.get(int(item.get('div', 1)), '전투력')} 랭킹 직업별 인구수 데이터 저장 완료: {len(class_data)}개 항목")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"직업별 인구수 데이터 저장 중 오류 발생: {e}")
        return False
    finally:
        db.close()




def generate_daily_statistics():
    """
    일별 통계 데이터를 생성하고 저장합니다.
    이 함수는 매일 자정에 실행되어야 합니다.
    mabinogi_ranking 테이블의 경우, 일일 평균 인구수, 직업 분포 등을 계산합니다.
    """
    db = SessionLocal()
    yesterday = (get_current_time() - timedelta(days=1)).date()
    
    try:
        # 각 서버별로 처리
        for server in SERVERS:
            # 서버별 인구수 통계 (전투력 랭킹 기준, div=1)
            # 전일 (24시간) 동안의 유니크한 캐릭터 수를 카운트
            pop_query = text("""
                SELECT 
                    COUNT(DISTINCT character_name) as population
                FROM 
                    mabinogi_ranking
                WHERE 
                    server_name = :server_name
                    AND div = 1
                    AND DATE(retrieved_at AT TIME ZONE 'Asia/Seoul') = :date_kst
            """)
            
            pop_result = db.execute(pop_query, {
                'server_name': server,
                'date_kst': yesterday
            }).fetchone()
            
            # 현재 인구수
            current_population = int(pop_result[0]) if pop_result and pop_result[0] else 0
            
            if current_population == 0:
                logger.warning(f"{server} 서버 {yesterday} 일자 인구수 데이터 없음")
                continue
            
            # 인구수 변화 계산 (전일 데이터 기준)
            day_before_yesterday = yesterday - timedelta(days=1)
            prev_pop_query = text("""
                SELECT avg_population
                FROM daily_server_stats
                WHERE server_name = :server_name AND date_kst = :date_kst
            """)
            
            prev_pop = db.execute(prev_pop_query, {
                'server_name': server,
                'date_kst': day_before_yesterday
            }).fetchone()
            
            population_change = 0
            if prev_pop and prev_pop[0]:
                population_change = current_population - int(prev_pop[0])
            
            # # 가장 인기 있는 직업 조회 (전투력 랭킹 기준, div=1)
            # popular_class_query = text("""
            #     SELECT 
            #         class_name, 
            #         COUNT(DISTINCT character_name) as char_count
            #     FROM 
            #         mabinogi_ranking
            #     WHERE 
            #         server_name = :server_name
            #         AND div = 1
            #         AND DATE(retrieved_at AT TIME ZONE 'Asia/Seoul') = :date_kst
            #     GROUP BY 
            #         class_name
            #     ORDER BY 
            #         char_count DESC
            #     LIMIT 1
            # """)
            
            # popular_class = db.execute(popular_class_query, {
            #     'server_name': server,
            #     'date_kst': yesterday
            # }).fetchone()
            
            # # 가장 높은 전투력을 가진 직업 조회
            # power_class_query = text("""
            #     SELECT 
            #         class_name, 
            #         AVG(power_value) as avg_power
            #     FROM 
            #         mabinogi_ranking
            #     WHERE 
            #         server_name = :server_name
            #         AND div = 1
            #         AND DATE(retrieved_at AT TIME ZONE 'Asia/Seoul') = :date_kst
            #     GROUP BY 
            #         class_name
            #     ORDER BY 
            #         avg_power DESC
            #     LIMIT 1
            # """)
            
            # power_class = db.execute(power_class_query, {
            #     'server_name': server,
            #     'date_kst': yesterday
            # }).fetchone()
            
            # 일별 통계 저장
            insert_query = text("""
                INSERT INTO daily_server_stats
                (server_name, date_kst, avg_population, max_population, min_population, 
                 population_change)
                VALUES
                (:server_name, :date_kst, :avg_population, :max_population, :min_population,
                 :population_change)
                ON CONFLICT (server_name, date_kst) 
                DO UPDATE SET
                    avg_population = :avg_population,
                    max_population = :max_population,
                    min_population = :min_population,
                    population_change = :population_change
                )
            """)
            
            db.execute(insert_query, {
                'server_name': server,
                'date_kst': yesterday,
                'avg_population': current_population,
                'max_population': current_population,  # 해당 일의 인구수
                'min_population': current_population,  # 해당 일의 인구수
                'population_change': population_change,
                # 'most_popular_class': popular_class[0] if popular_class else None,
                # 'top_power_class': power_class[0] if power_class else None
            })
        
        db.commit()
        logger.info(f"{yesterday} 일자 일별 통계 생성 완료 (KST 기준)")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"일별 통계 생성 중 오류 발생: {e}")
        return False
    finally:
        db.close()

def update_population_statistics():
    """
    인구수 통계 데이터를 수집하고 저장하는 메인 함수
    mabinogi_ranking 테이블에서 각 랭킹 타입별(div 값에 따라 전투력, 매력, 생활력)로 데이터를 수집합니다.
    """
    try:
        # DB 테이블 초기화
        if not initialize_database():
            logger.error("데이터베이스 초기화 실패")
            return False
        
        # 1. 서버별 전체 인구수 수집 및 저장 (기존 로직 유지)
        server_data = get_all_server_populations()
        if server_data:
            save_server_population(server_data)
        
        # 6. 일별 통계 생성 (필요한 경우)
        current_time = get_current_time()
        current_hour = current_time.hour
        
        if current_hour == 0:  # 자정에 실행
            generate_daily_statistics()
            logger.info(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 일별 통계 생성 완료")
        
        logger.info(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 인구수 통계 데이터 업데이트 완료")
        return True
    except Exception as e:
        logger.error(f"인구수 통계 업데이트 중 오류 발생: {e}")
        return False
