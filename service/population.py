import concurrent.futures
import matplotlib.pyplot as plt
import numpy as np
import os
import logging
from datetime import datetime, timedelta
import pytz
from sqlalchemy import text
from service.db_session import SessionLocal, get_current_time, KST
from service.full_data import fetch_rank_via_requests, parse_rank_html
from service.db import insert_data

# 로거 설정
logger = logging.getLogger(__name__)

def get_last_characters():
    """
    각 서버별 가장 하위 랭킹 캐릭터를 조회합니다.
    
    Returns:
        list: 각 서버별 최하위 캐릭터 정보 리스트
    """
    db = SessionLocal()
    try:
        # 각 서버별 최대 랭킹 포지션을 가진 캐릭터 조회
        query = text("""
            SELECT
              mr.*
            FROM
              mabinogi_ranking mr
            INNER JOIN (
              SELECT
                server_name,
                MAX(rank_position) AS max_rank_position
              FROM
                mabinogi_ranking
              GROUP BY
                server_name
            ) AS max_ranks
              ON mr.server_name = max_ranks.server_name
              AND mr.rank_position = max_ranks.max_rank_position
            ORDER BY
              mr.server_name
        """)
        
        results = db.execute(query).fetchall()
        
        # 결과를 딕셔너리 리스트로 변환
        columns = ['id', 'rank_position', 'change_amount', 'change_type', 
                  'server_name', 'character_name', 'class_name', 'power_value', 'retrieved_at', 'div']
        return [{columns[i]: value for i, value in enumerate(row)} for row in results]
    except Exception as e:
        logger.error(f"서버별 최하위 캐릭터 조회 중 오류 발생: {e}")
        return []
    finally:
        db.close()

def find_server_population(server_name, last_character):
    """
    주어진 서버에서 마지막 캐릭터보다 더 낮은 랭킹의 캐릭터를 찾아 실제 인구수를 확인합니다.
    
    Args:
        server_name (str): 서버 이름
        last_character (dict): 최하위 캐릭터 정보
        
    Returns:
        int: 서버의 추정 인구수
    """
    try:
        # 현재 저장된 최하위 캐릭터의 랭킹
        last_rank = last_character['rank_position']
        character_name = last_character['character_name']
        
        # 마지막 캐릭터 정보 가져오기
        html_data = fetch_rank_via_requests(server_name, character_name)
        parsed_data = parse_rank_html(html_data)
        
        # 가져온 데이터를 DB에 저장 (전투력 랭킹 div=1)
        if parsed_data:
            insert_data(parsed_data, server=server_name, div=1)
            logger.info(f"{server_name} 서버 인구수 확인 중 {len(parsed_data)}개 캐릭터 데이터 DB 저장")
        
        # 파싱된 데이터에서 현재 캐릭터의 랭킹 확인
        current_rank = last_rank
        for item in parsed_data:
            if item['character'] == character_name:
                current_rank = int(item['rank'].replace(',', '').replace('위', ''))
                break
        
        # 현재 랭킹이 마지막 저장된 랭킹보다 높으면 더 크롤링 필요 없음
        if current_rank < last_rank:
            return current_rank
            
        return last_rank
    except Exception as e:
        logger.error(f"{server_name} 서버 인구수 확인 중 오류 발생: {e}")
        return last_rank  # 오류 발생 시 기존 랭킹 반환

def get_all_server_populations():
    """
    모든 서버의 인구수를 멀티스레드로 확인합니다.
    
    Returns:
        list: 서버별 인구수 정보 리스트
    """
    # 각 서버별 최하위 캐릭터 정보 가져오기
    last_characters = get_last_characters()
    
    if not last_characters:
        return []
    
    # 서버별 인구수 확인 작업 멀티스레드로 실행
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_server = {
            executor.submit(find_server_population, char['server_name'], char): char['server_name']
            for char in last_characters
        }
        
        for future in concurrent.futures.as_completed(future_to_server):
            server_name = future_to_server[future]
            try:
                population = future.result()
                results.append({
                    "server_name": server_name, 
                    "population": population
                })
                # logger.info(f"{server_name} 서버 인구수: {population}")
            except Exception as e:
                logger.error(f"{server_name} 서버 인구수 확인 결과 처리 중 오류: {e}")
    
    # 서버 이름 순으로 정렬
    results.sort(key=lambda x: x["server_name"])
    return results

def generate_population_graph(population_data):
    """
    인구수 데이터를 기반으로 그래프 이미지를 생성합니다.
    
    Args:
        population_data (list): 서버별 인구수 정보 리스트
        
    Returns:
        str: 생성된 이미지 파일 경로
    """
    try:
        # 백그라운드 스레드에서 Matplotlib 사용을 위한 설정
        import matplotlib
        matplotlib.use('Agg')  # GUI 없이 이미지만 생성하는 모드
        
        # 한글 폰트 설정
        plt.rcParams['font.family'] = 'Malgun Gothic'
        plt.rcParams['axes.unicode_minus'] = False
        
        # 데이터 준비
        servers = [item['server_name'] for item in population_data]
        populations = [item['population'] for item in population_data]
        
        # 그래프 크기 설정 (가로 480px)
        plt.figure(figsize=(4.8, 3.2))
        
        # 숫자 포맷팅 함수 정의 (K, M 단위 표시)
        def format_number(x, pos):
            if x >= 1000000:
                return f'{x/1000000:.1f}M'  # 백만 단위는 M으로 표시
            elif x >= 1000:
                return f'{x/1000:.0f}K'     # 천 단위는 K로 표시
            else:
                return f'{x:.0f}'
        
        # 바 차트 생성
        bars = plt.bar(servers, populations, color='skyblue')
        
        # 바 위에 숫자 표시 (포맷팅 적용)
        for bar, population in zip(bars, populations):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
                     f'{population:,}',
                     ha='center', va='bottom', fontsize=10)
        
        # 그래프 타이틀 및 레이블 설정
        plt.xlabel('서버', fontsize=12)
        plt.ylabel('인구수', fontsize=12)
        plt.xticks(rotation=45)
        plt.ylim(0, max(populations) * 1.2)  # y축 범위 설정
        
        # y축 포맷터 설정
        from matplotlib.ticker import FuncFormatter
        plt.gca().yaxis.set_major_formatter(FuncFormatter(format_number))
        
        plt.tight_layout()
        
        # 이미지 저장
        image_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "images")
        os.makedirs(image_dir, exist_ok=True)
        
        # 파일명에 현재 시간 포함
        timestamp = datetime.now(KST).strftime("%Y%m%d%H%M%S")
        filename = f"population_graph_{timestamp}.png"
        filepath = os.path.join(image_dir, filename)
        
        plt.savefig(filepath, dpi=100)
        plt.close('all')  # 모든 플롯 닫기
        
        return filename
    except Exception as e:
        logger.error(f"인구수 그래프 생성 중 오류 발생: {e}")
        return None

# 캐싱을 위한 변수들
_population_cache = {
    "data": None,
    "timestamp": None,
    "imageUrl": None,
    "cache_time": None,
    "last_updated": None
}

def get_population_data():
    """
    인구수 데이터와 그래프 이미지를 반환합니다.
    1시간마다 백그라운드에서 자동으로 갱신되며, 요청시 최신 데이터를 반환합니다.
    
    Returns:
        dict: 인구수 데이터와 그래프 이미지 URL
    """
    global _population_cache
    
    current_time = get_current_time()
    
    # 캐시된 데이터가 있으면 반환 
    if _population_cache["data"]:
        
        logger.info(f"캐시된 인구수 데이터 반환 (마지막 업데이트: {_population_cache['last_updated'].strftime('%Y-%m-%d %H:%M:%S KST')})")        
        return {
            "success": True,
            "data": _population_cache["data"],
            "imageUrl": _population_cache["imageUrl"],
            "timestamp": _population_cache["timestamp"],
            "from_cache": True,
            "last_updated": _population_cache["last_updated"].strftime('%Y-%m-%d %H:%M:%S KST')
        }
    
    try:
        # 서버별 인구수 조회
        population_data = get_all_server_populations()
        
        if not population_data:
            return {"success": False, "message": "인구수 데이터를 조회할 수 없습니다."}
        
        # 그래프 이미지 생성
        image_filename = generate_population_graph(population_data)
        
        if not image_filename:
            return {
                "success": True,
                "data": population_data,
                "message": "그래프 이미지 생성에 실패했습니다."
            }
        
        # 현재 시간을 KST로 포맷팅
        timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S KST')
        image_url = f"/images/{image_filename}"
        
        # 캐시 업데이트
        _population_cache["data"] = population_data
        _population_cache["timestamp"] = timestamp
        _population_cache["imageUrl"] = image_url
        _population_cache["cache_time"] = current_time
        _population_cache["last_updated"] = current_time
        
        # 결과 반환
        return {
            "success": True,
            "data": population_data,
            "imageUrl": image_url,
            "timestamp": timestamp,
            "from_cache": False
        }
    except Exception as e:
        logger.error(f"인구수 데이터 처리 중 오류 발생: {e}")
        return {"success": False, "message": f"오류 발생: {str(e)}"}
