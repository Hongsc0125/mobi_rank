import logging
import os
import concurrent.futures
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import text
from service.db_session import SessionLocal, get_current_time, KST
from service.full_data import fetch_rank_via_requests, parse_rank_html, switch_server
from service.population import get_all_server_populations

# 로거 설정
logger = logging.getLogger(__name__)

# 서버 목록
SERVERS = ["데이안", "아이라", "던컨", "알리사", "메이븐", "라사", "칼릭스"]

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

def fetch_class_data_for_server(server_name, sample_size=1000):
    """
    특정 서버의 직업별 인구수 데이터를 수집합니다.
    
    Args:
        server_name (str): 서버 이름
        sample_size (int): 샘플링할 캐릭터 수 (랭킹 1~sample_size)
    
    Returns:
        dict: 직업별 데이터 (캐릭터 수, 평균 전투력 등)
    """
    try:
        # 빈 검색어로 상위 랭킹 조회
        html_data = fetch_rank_via_requests(server_name, "")
        parsed_data = parse_rank_html(html_data)
        
        if not parsed_data:
            logger.warning(f"{server_name} 서버 데이터 조회 실패")
            return None
        
        # 클래스별 데이터 집계
        class_data = defaultdict(lambda: {'count': 0, 'total_power': 0})
        total_characters = len(parsed_data)
        
        for char in parsed_data:
            class_name = char['class']
            # 전투력에서 콤마 제거하고 정수로 변환
            power = int(char['power'].replace(',', ''))
            
            class_data[class_name]['count'] += 1
            class_data[class_name]['total_power'] += power
        
        # 각 직업별 평균 전투력 계산 및 백분율 추가
        result = []
        for class_name, data in class_data.items():
            result.append({
                'server_name': server_name,
                'class_name': class_name,
                'character_count': data['count'],
                'percentage': round((data['count'] / total_characters) * 100, 2),
                'average_power': int(data['total_power'] / data['count']) if data['count'] > 0 else 0
            })
        
        # 캐릭터 수 기준 내림차순 정렬
        result.sort(key=lambda x: x['character_count'], reverse=True)
        return result
    
    except Exception as e:
        logger.error(f"{server_name} 서버 직업별 데이터 수집 중 오류 발생: {e}")
        return None

def collect_class_data_all_servers():
    """
    모든 서버의 직업별 인구수 데이터를 수집합니다.
    
    Returns:
        list: 모든 서버의 직업별 데이터
    """
    all_data = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_server = {
            executor.submit(fetch_class_data_for_server, server): server
            for server in SERVERS
        }
        
        for future in concurrent.futures.as_completed(future_to_server):
            server = future_to_server[future]
            try:
                data = future.result()
                if data:
                    all_data.extend(data)
                    logger.info(f"{server} 서버 직업별 데이터 수집 완료: {len(data)}개 직업")
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
        logger.info(f"직업별 인구수 데이터 저장 완료: {len(class_data)}개 항목")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"직업별 인구수 데이터 저장 중 오류 발생: {e}")
        return False
    finally:
        db.close()

def collect_power_distribution(server_name, sample_size=1000):
    """
    특정 서버의 전투력 분포 데이터를 수집합니다.
    
    Args:
        server_name (str): 서버 이름
        sample_size (int): 샘플링할 캐릭터 수 (랭킹 1~sample_size)
    
    Returns:
        list: 전투력 범위별 데이터
    """
    try:
        # 빈 검색어로 상위 랭킹 조회
        html_data = fetch_rank_via_requests(server_name, "")
        parsed_data = parse_rank_html(html_data)
        
        if not parsed_data:
            logger.warning(f"{server_name} 서버 데이터 조회 실패")
            return None
        
        # 모든 전투력 값 추출
        powers = [int(char['power'].replace(',', '')) for char in parsed_data]
        
        if not powers:
            return None
        
        # 전투력 범위 설정 (데이터 기반으로 적절한 구간 설정)
        min_power = min(powers)
        max_power = max(powers)
        
        # 범위를 10개 구간으로 나누기
        range_size = (max_power - min_power) // 10
        if range_size == 0:  # 데이터가 적거나 모두 같은 값인 경우
            range_size = 10000  # 기본값 설정
        
        # 각 구간별 캐릭터 수 계산
        distribution = defaultdict(int)
        
        for power in powers:
            range_start = (power // range_size) * range_size
            range_end = range_start + range_size - 1
            distribution[(range_start, range_end)] += 1
        
        # 결과 포맷팅
        total_characters = len(powers)
        result = []
        for (range_start, range_end), count in sorted(distribution.items()):
            result.append({
                'server_name': server_name,
                'power_range_start': range_start,
                'power_range_end': range_end,
                'character_count': count,
                'percentage': round((count / total_characters) * 100, 2)
            })
        
        return result
    
    except Exception as e:
        logger.error(f"{server_name} 서버 전투력 분포 수집 중 오류 발생: {e}")
        return None

def collect_power_distribution_all_servers():
    """
    모든 서버의 전투력 분포 데이터를 수집합니다.
    
    Returns:
        list: 모든 서버의 전투력 분포 데이터
    """
    all_data = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_server = {
            executor.submit(collect_power_distribution, server): server
            for server in SERVERS
        }
        
        for future in concurrent.futures.as_completed(future_to_server):
            server = future_to_server[future]
            try:
                data = future.result()
                if data:
                    all_data.extend(data)
                    logger.info(f"{server} 서버 전투력 분포 수집 완료: {len(data)}개 구간")
            except Exception as e:
                logger.error(f"{server} 서버 전투력 분포 처리 중 오류: {e}")
    
    return all_data

def save_power_distribution(power_data):
    """
    전투력 분포 데이터를 DB에 저장합니다.
    
    Args:
        power_data (list): 전투력 분포 정보 리스트
    """
    if not power_data:
        return False
    
    db = SessionLocal()
    current_time = get_current_time()
    
    try:
        for item in power_data:
            query = text("""
                INSERT INTO power_distribution_stats
                (server_name, power_range_start, power_range_end, character_count, percentage, retrieved_at) 
                VALUES (:server_name, :power_range_start, :power_range_end, :character_count, :percentage, :retrieved_at)
            """)
            
            db.execute(query, {
                'server_name': item['server_name'],
                'power_range_start': item['power_range_start'],
                'power_range_end': item['power_range_end'],
                'character_count': item['character_count'],
                'percentage': item['percentage'],
                'retrieved_at': current_time
            })
        
        db.commit()
        logger.info(f"전투력 분포 데이터 저장 완료: {len(power_data)}개 항목")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"전투력 분포 데이터 저장 중 오류 발생: {e}")
        return False
    finally:
        db.close()

def generate_daily_statistics():
    """
    일별 통계 데이터를 생성하고 저장합니다.
    이 함수는 매일 자정에 실행되어야 합니다.
    """
    db = SessionLocal()
    yesterday = (get_current_time() - timedelta(days=1)).date()
    
    try:
        # 각 서버별로 처리
        for server in SERVERS:
            # 일일 서버 인구수 통계
            pop_query = text("""
                SELECT 
                    AVG(population) as avg_population,
                    MAX(population) as max_population,
                    MIN(population) as min_population
                FROM server_population_stats
                WHERE 
                    server_name = :server_name AND
                    DATE(retrieved_at AT TIME ZONE 'Asia/Seoul') = :date_kst
            """)
            
            pop_stats = db.execute(pop_query, {
                'server_name': server,
                'date_kst': yesterday
            }).fetchone()
            
            if not pop_stats or pop_stats[0] is None:
                logger.warning(f"{server} 서버 {yesterday} 일자 인구수 데이터 없음")
                continue
            
            # 인구수 변화 계산 (전일 평균 대비)
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
                population_change = int(pop_stats[0]) - int(prev_pop[0])
            
            # 가장 인기 있는 직업 조회
            popular_class_query = text("""
                SELECT class_name
                FROM class_population_stats
                WHERE 
                    server_name = :server_name AND
                    DATE(retrieved_at AT TIME ZONE 'Asia/Seoul') = :date_kst
                GROUP BY class_name
                ORDER BY AVG(character_count) DESC
                LIMIT 1
            """)
            
            popular_class = db.execute(popular_class_query, {
                'server_name': server,
                'date_kst': yesterday
            }).fetchone()
            
            # 가장 높은 전투력을 가진 직업 조회
            power_class_query = text("""
                SELECT class_name
                FROM class_population_stats
                WHERE 
                    server_name = :server_name AND
                    DATE(retrieved_at AT TIME ZONE 'Asia/Seoul') = :date_kst
                GROUP BY class_name
                ORDER BY AVG(average_power) DESC
                LIMIT 1
            """)
            
            power_class = db.execute(power_class_query, {
                'server_name': server,
                'date_kst': yesterday
            }).fetchone()
            
            # 일별 통계 저장
            insert_query = text("""
                INSERT INTO daily_server_stats
                (server_name, date_kst, avg_population, max_population, min_population, 
                 population_change, most_popular_class, top_power_class)
                VALUES
                (:server_name, :date_kst, :avg_population, :max_population, :min_population,
                 :population_change, :most_popular_class, :top_power_class)
                ON CONFLICT (server_name, date_kst) 
                DO UPDATE SET
                    avg_population = :avg_population,
                    max_population = :max_population,
                    min_population = :min_population,
                    population_change = :population_change,
                    most_popular_class = :most_popular_class,
                    top_power_class = :top_power_class
            """)
            
            db.execute(insert_query, {
                'server_name': server,
                'date_kst': yesterday,
                'avg_population': int(pop_stats[0]) if pop_stats[0] else 0,
                'max_population': int(pop_stats[1]) if pop_stats[1] else 0,
                'min_population': int(pop_stats[2]) if pop_stats[2] else 0,
                'population_change': population_change,
                'most_popular_class': popular_class[0] if popular_class else None,
                'top_power_class': power_class[0] if power_class else None
            })
        
        db.commit()
        logger.info(f"{yesterday} 일자 일별 통계 생성 완료")
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
    """
    try:
        # DB 테이블 초기화
        if not initialize_database():
            logger.error("데이터베이스 초기화 실패")
            return False
        
        # 1. 서버별 전체 인구수 수집 및 저장
        server_data = get_all_server_populations()
        if server_data:
            save_server_population(server_data)
        
        # 2. 직업별 인구수 수집 및 저장
        class_data = collect_class_data_all_servers()
        if class_data:
            save_class_population_data(class_data)
        
        # 3. 전투력 분포 수집 및 저장
        power_data = collect_power_distribution_all_servers()
        if power_data:
            save_power_distribution(power_data)
        
        # 4. 일별 통계 생성 (필요한 경우)
        current_hour = get_current_time().hour
        if current_hour == 0:  # 자정에 실행
            generate_daily_statistics()
        
        logger.info("인구수 통계 데이터 업데이트 완료")
        return True
    except Exception as e:
        logger.error(f"인구수 통계 업데이트 중 오류 발생: {e}")
        return False
