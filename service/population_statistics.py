import logging
from datetime import datetime
import os
import pytz
import json
import pandas as pd
import numpy as np
import time
from io import BytesIO
from pathlib import Path
from jinja2 import Template, Environment, select_autoescape
from sqlalchemy import text
from service.db_session import SessionLocal, KST, get_current_time
import asyncio
from api.rankData import get_all_ranks_data
from service.population import get_all_server_populations, _population_cache
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import chromedriver_autoinstaller

logger = logging.getLogger(__name__)

# 직업별 인구 통계 캐시
_class_population_cache = {
    "data": None,          # 직업별 인구 데이터
    "timestamp": None,     # 타임스탬프
    "imageUrl": None,      # 전체 이미지 URL
    "chartImageUrl": None, # 차트 이미지 URL
    "tableImageUrl": None, # 테이블 이미지 URL
    "total_population": 0, # 전체 인구수
    "jobs": [],           # 직업별 인구 데이터 목록
    "last_updated": None,  # 마지막 업데이트 시간
    "cache_time": None     # 캐시 시간
}

def update_population_statistics():
    """
    매일 00시에 전체/직업별/서버별/서버별-직업별 인구수를 통계 테이블에 저장합니다.
    PostgreSQL one query 방식(INSERT ... SELECT ... UNION ALL)
    """
    session = SessionLocal()
    try:
        # PostgreSQL 한번의 쿼리로 모든 인구 통계 저장
        sql = """
            INSERT INTO mabinogi_population_statistics
                (date, server_name, class_name, population, retrieved_at, div)
            SELECT
                CURRENT_DATE,
                'all' AS server_name,
                'all' AS class_name,
                COUNT(*) AS population,
                (now() AT TIME ZONE 'Asia/Seoul') AS retrieved_at,
                1 AS div
            FROM mabinogi_ranking
            UNION ALL
            SELECT
                CURRENT_DATE,
                'all',
                class_name,
                COUNT(*),
                (now() AT TIME ZONE 'Asia/Seoul'),
                1
            FROM mabinogi_ranking
            GROUP BY class_name
            UNION ALL
            SELECT
                CURRENT_DATE,
                server_name,
                'all',
                COUNT(*),
                (now() AT TIME ZONE 'Asia/Seoul'),
                1
            FROM mabinogi_ranking
            GROUP BY server_name
            UNION ALL
            SELECT
                CURRENT_DATE,
                server_name,
                class_name,
                COUNT(*),
                (now() AT TIME ZONE 'Asia/Seoul'),
                1
            FROM mabinogi_ranking
            GROUP BY server_name, class_name
            ON CONFLICT (date, server_name, class_name, div) DO NOTHING;
        """
        
        logger.info("인구수 통계 집계 쿼리 실행 시작")
        session.execute(text(sql))
        session.commit()
        logger.info("인구수 통계 집계 쿼리 성공적으로 실행됨")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"[인구수 통계 집계 쿼리 오류]\n쿼리: {sql}\n에러: {e}")
        return False
    finally:
        session.close()


def get_total_population_from_server_data():
    """
    서버별 인구 데이터를 합쳐 전체 인구수를 계산합니다.
    """
    try:
        # 캩0시에서 서버별 인구 데이터 가져오기
        cache_data = _population_cache.get("data")
        
        # 캐시에 데이터가 없으면 가져오기
        if not cache_data:
            cache_response = get_all_server_populations()
            if cache_response and "data" in cache_response:
                cache_data = cache_response.get("data")
            else:
                cache_data = cache_response
        
        # 데이터가 없거나 유효하지 않은 경우
        if not cache_data:
            return 0
        
        # 로그로 데이터 형식 확인
        logger.info(f"서버 데이터 형식: {type(cache_data).__name__}")
        
        # 데이터 형식에 따라 전체 인구수 계산
        total_population = 0
        
        if isinstance(cache_data, list):  # 리스트 형태인 경우
            for server in cache_data:
                if isinstance(server, dict) and 'population' in server:
                    total_population += server.get('population', 0)
        elif isinstance(cache_data, dict):  # 딕셔너리 형태인 경우
            for server_data in cache_data.values():
                if isinstance(server_data, dict):
                    total_population += server_data.get('population', 0)
        
        logger.info(f"서버별 인구 계산 결과: 총 {total_population:,} 명")
        return total_population
    except Exception as e:
        logger.error(f"서버별 인구 데이터에서 전체 인구수 계산 중 오류: {e}")
        return 0

def get_total_population_from_database():
    """
    드롭다운에서 단순하게 가져오기 위한 전체 인구수를 데이터베이스에서 가져옵니다.
    """
    session = SessionLocal()
    try:
        # 가장 최근 날짜의 전체 인구수 가져오기
        sql = """
            SELECT population
            FROM mabinogi_population_statistics
            WHERE date = (SELECT MAX(date) FROM mabinogi_population_statistics)
            AND server_name = 'all' AND class_name = 'all'
        """
        
        result = session.execute(text(sql)).fetchone()
        if result and result[0]:
            return result[0]
        return 0
    except Exception as e:
        logger.error(f"전체 인구수 조회 중 오류: {e}")
        return 0
    finally:
        session.close()

def get_class_population_data(date=None):
    """
    특정 날짜의 직업별 인구수 데이터를 가져옵니다.
    날짜가 지정되지 않으면 가장 최근 날짜의 데이터를 가져옵니다.
    
    Args:
        date (str, optional): 데이터를 조회할 날짜 (YYYY-MM-DD 형식)
        
    Returns:
        dict: 직업별 인구수 데이터
    """
    session = SessionLocal()
    try:
        # 날짜 설정
        date_condition = ""
        if date:
            date_condition = f"WHERE date = '{date}'"
        else:
            # 가장 최근 날짜 가져오기
            date_condition = "WHERE date = (SELECT MAX(date) FROM mabinogi_population_statistics)"
        
        # 직업별 인구수 쿼리
        sql = f"""
            SELECT class_name, population 
            FROM mabinogi_population_statistics 
            {date_condition} AND server_name = 'all' AND class_name != 'all'
            ORDER BY population DESC
        """
        
        result = session.execute(text(sql)).fetchall()
        
        # 데이터 정제 (직업명이 '견습'으로 시작하는 경우 제외 또는 합치기 옵션)
        class_data = {}
        for row in result:
            class_name = row[0]
            population = row[1]
            
            # '견습'으로 시작하는 직업은 제외하지 않고 그대로 포함
            class_data[class_name] = population
        
        return class_data
    except Exception as e:
        logger.error(f"직업별 인구수 데이터 조회 중 오류: {e}")
        return {}
    finally:
        session.close()


def get_html_template():
    """
    직업별 인구수 통계를 표시할 HTML 템플릿을 반환합니다.
    templates 디렉토리에서 HTML 파일을 읽어옵니다.
    """
    try:
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates", "class_chart_template.html")
        
        # 템플릿 파일 읽기
        with open(template_path, 'r', encoding='utf-8') as template_file:
            template_content = template_file.read()
            
        logger.info(f"HTML 템플릿 파일 로드 성공: {template_path}")
        return template_content
    except Exception as e:
        logger.error(f"HTML 템플릿 파일 로드 실패: {e}")
        return None


def take_screenshots(html_content, base_output_path):
    """
    HTML 콘텐츠를 렌더링하고 차트와 테이블을 별도로 스크린샷을 찍습니다.
    
    Args:
        html_content (str): 렌더링할 HTML 콘텐츠
        base_output_path (str): 스크린샷 저장 경로 (확장자 제외)
        
    Returns:
        dict: 생성된 파일명 디렉토리
    """
    try:
        # 임시 HTML 파일 생성
        temp_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "temp")
        os.makedirs(temp_dir, exist_ok=True)
        temp_html_path = os.path.join(temp_dir, "temp_chart.html")
        
        with open(temp_html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # ChromeDriver 설치 확인 및 경로 가져오기
        chromedriver_path = chromedriver_autoinstaller.install()
        
        # Chrome 옵션 설정
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 헤드리스 모드
        chrome_options.add_argument('--disable-gpu')  # Windows에서 필요한 경우가 있음
        chrome_options.add_argument('--window-size=1600,2000')  # 더 큰 윈도우 크기 설정
        chrome_options.add_argument('--no-sandbox')  # 일부 환경에서 필요
        chrome_options.add_argument('--disable-dev-shm-usage')  # 메모리 문제 방지
        
        # Chrome 드라이버 실행
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # HTML 파일 로드
        file_url = f"file:///{temp_html_path.replace(os.sep, '/')}"  # 파일 경로를 URL로 변환
        driver.get(file_url)
        
        # 차트가 렌더링될 시간 기다리기
        time.sleep(2)
        
        # 결과 파일들
        results = {}
        
        # 1. 전체 페이지 스크린샷
        full_path = f"{base_output_path}.png"
        driver.save_screenshot(full_path)
        results['full'] = os.path.basename(full_path)
        
        # 2. 파이 차트만 캡처
        driver.execute_script("window.scrollTo(0, 0);")
        chart_element = driver.find_element('css selector', '.chart-container')
        chart_path = f"{base_output_path}_chart.png"
        
        # 차트 영역만 캡처
        chart_element.screenshot(chart_path)
        results['chart'] = os.path.basename(chart_path)
        
        # 3. 테이블만 캡처
        table_element = driver.find_element('css selector', 'table')
        driver.execute_script("arguments[0].scrollIntoView();", table_element)
        time.sleep(0.5)
        
        table_path = f"{base_output_path}_table.png"
        table_element.screenshot(table_path)
        results['table'] = os.path.basename(table_path)
        
        # 브라우저 종료
        driver.quit()
        
        # 임시 파일 삭제
        if os.path.exists(temp_html_path):
            os.remove(temp_html_path)
            
        return results
    except Exception as e:
        logger.error(f"스크린샷 생성 중 오류 발생: {e}")
        return None

def generate_class_pie_chart():
    """
    직업별 인구수 비율을 HTML로 생성하고 스크린샷을 찍어 저장합니다.
    
    Returns:
        tuple: (생성된 이미지 파일명, 직업별 인구수 데이터)
    """
    try:
        # 직업별 인구수 데이터 가져오기
        class_data = get_class_population_data()
        
        if not class_data:
            logger.error("직업별 인구수 데이터가 없습니다.")
            return None
        
        # 데이터프레임으로 변환
        df = pd.DataFrame(list(class_data.items()), columns=['job', 'population'])
        df = df.sort_values(by='population', ascending=False)  # 인구수 기준 내림차순 정렬
        
        # 직업별 데이터 합계로 전체 인구수 계산
        total_from_classes = df['population'].sum()
        
        # 서버별 인구 데이터에서 전체 인구수 계산
        total_from_servers = get_total_population_from_server_data()
        
        # 데이터베이스에서 전체 인구수 조회
        total_from_db = get_total_population_from_database()
        
        # 전체 인구수 선택 - 우선순위: 서버데이터 > DB > 직업별합계
        total_population = total_from_servers
        if total_population <= 0:
            total_population = total_from_db
        if total_population <= 0:
            total_population = total_from_classes
        
        # 로그 출력 (KST 시간 기준)
        logger.info(f"전체 인구수 (서버별 합계): {total_from_servers:,} 명")
        logger.info(f"전체 인구수 (DB): {total_from_db:,} 명")
        logger.info(f"전체 인구수 (직업별 합계): {total_from_classes:,} 명")
        logger.info(f"최종 사용할 전체 인구수: {total_population:,} 명")
        
        # 기존 데이터의 백분율 계산 (직업별 합계 기준)
        df['percentage'] = df['population'] / total_from_classes * 100
        
        # 원래 백분율 보존하면서, 새로운 전체 인구수 기준으로 직업별 인구수 재계산
        df['recalculated_population'] = df['percentage'] * total_population / 100
        
        # 인구수 칼럼 갱신 - 재계산된 인구수로 대체
        df['population'] = df['recalculated_population'].round().astype(int)
        
        # 현재 시간 (KST)
        current_time = get_current_time()
        date_str = current_time.strftime('%Y-%m-%d')
        timestamp_str = current_time.strftime('%Y-%m-%d %H:%M:%S KST')
        
        # 템플릿 데이터 준비
        job_names = df['job'].tolist()
        job_populations = df['population'].tolist()
        job_percentages = df['percentage'].round(2).tolist()
        
        job_data = []
        for idx, row in df.iterrows():
            job_data.append({
                'name': row['job'],
                'population': row['population'],
                'population_formatted': '{:,}'.format(row['population']),
                'percentage': row['percentage']
            })
            
        # 로그에 재계산된 인구 정보 출력
        logger.info("직업별 인구수 재계산 결과:")
        for _, row in df.head(5).iterrows():
            logger.info(f"  - {row['job']}: {row['percentage']:.2f}% => {row['population']:,} 명")
        
        # 천 단위 구분 포맷팅 문자열 미리 생성
        total_population_formatted = '{:,}'.format(total_population)
        
        # 템플릿 가져오기
        template_str = get_html_template()
        
        # Jinja2 기본 템플릿 사용
        template = Template(template_str)
        
        # 템플릿 렌더링
        html_content = template.render(
            date=date_str,
            timestamp=timestamp_str,
            total_population=total_population,
            total_population_formatted=total_population_formatted,
            job_data=job_data,
            job_names=job_names,
            job_populations=job_populations,
            job_percentages=job_percentages
        )
        
        # 이미지 저장 경로
        image_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "images")
        os.makedirs(image_dir, exist_ok=True)
        
        # 파일명에 현재 날짜 포함
        date_timestamp = current_time.strftime("%Y%m%d")
        base_filename = f"class_distribution_{date_timestamp}"
        base_filepath = os.path.join(image_dir, base_filename)
        
        # HTML 파일 저장
        html_filename = f"{base_filename}.html"
        html_filepath = os.path.join(image_dir, html_filename)
        
        with open(html_filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        logger.info(f"직업별 인구수 차트 HTML 파일 저장 완료: {html_filename}")
        
        # HTML 렌더링하고 여러 스크린샷 찍기
        screenshot_results = take_screenshots(html_content, base_filepath)
        
        if screenshot_results:
            logger.info(f"직업별 인구수 차트 스크린샷 생성 완료:")
            for img_type, img_name in screenshot_results.items():
                logger.info(f"  - {img_type}: {img_name}")
            
            # 기존 직업별 인구 파일들 정리 (최신 파일들 제외)
            cleanup_old_class_distribution_images(image_dir, base_filename)
            
            # 직업별 인구수 데이터 반환을 위한 준비
            job_result_data = []
            for row_data in job_data:
                job_result_data.append({
                    "job_name": row_data['name'],
                    "population": row_data['population'],
                    "percentage": round(row_data['percentage'], 2)
                })
            
            # 전체 스크린샷 파일명과 직업별 인구수 데이터 반환
            result_data = {
                "filename": screenshot_results['full'],
                "chart_filename": screenshot_results['chart'],
                "table_filename": screenshot_results['table'],
                "total_population": total_population,
                "timestamp": timestamp_str,
                "jobs": job_result_data
            }
            return screenshot_results['full'], result_data
        else:
            logger.error("직업별 인구수 차트 스크린샷 생성 실패")
            return None, None
    except Exception as e:
        logger.error(f"직업별 인구수 차트 생성 중 오류 발생: {e}")
        return None, None

def cleanup_old_class_distribution_images(image_dir, keep_base_filename):
    """
    직업별 인구 분포 이미지 파일들 중 최신 파일들을 제외하고 모두 삭제합니다.
    
    Args:
        image_dir (str): 이미지 디렉토리 경로
        keep_base_filename (str): 유지할 기본 파일명 (확장자 제외)
    """
    try:
        # class_distribution_으로 시작하는 파일들 찾기
        class_files = [f for f in os.listdir(image_dir) if f.startswith('class_distribution_')]
        
        # 유지할 파일들 패턴 (base_filename으로 시작하는 모든 파일)
        keep_pattern = os.path.basename(keep_base_filename)
        
        # 유지할 파일들을 제외한 나머지 삭제
        deleted_count = 0
        for filename in class_files:
            # 확장자를 제거한 파일명으로 비교
            file_base = filename.rsplit('.', 1)[0]
            if not file_base.startswith(keep_pattern):
                file_path = os.path.join(image_dir, filename)
                try:
                    os.remove(file_path)
                    deleted_count += 1
                    logger.info(f"기존 직업별 인구 파일 삭제: {filename}")
                except OSError as e:
                    logger.warning(f"파일 삭제 실패: {filename}, 오류: {e}")
        
        if deleted_count > 0:
            logger.info(f"직업별 인구 파일 정리 완료: {deleted_count}개 파일 삭제됨")
    except Exception as e:
        logger.error(f"직업별 인구 파일 정리 중 오류: {e}")

def get_latest_class_chart():
    """
    가장 최근에 생성된 직업별 인구수 파이 차트 데이터를 반환합니다.
    캐시된 데이터가 있으면 캐시 데이터를 반환하고, 없으면 파일에서 가져옵니다.
    
    Returns:
        dict: 직업별 인구수 차트 데이터 (캐시된 데이터가 있는 경우) 또는 None
    """
    try:
        # 캐시된 데이터가 있는지 확인
        if _class_population_cache["data"] and _class_population_cache["imageUrl"]:
            # 캐시에서 데이터 가져오기
            logger.info(f"캐시된 직업별 인구수 차트 데이터 반환 (캐시 시간: {_class_population_cache['timestamp']})")
            
            return {
                "success": True,
                "message": "직업별 인구 통계 정보 가져오기 성공",
                "data": _class_population_cache["jobs"],
                "total_population": _class_population_cache["total_population"],
                "imageUrl": _class_population_cache["imageUrl"],
                "chartImageUrl": _class_population_cache["chartImageUrl"],
                "tableImageUrl": _class_population_cache["tableImageUrl"],
                "timestamp": _class_population_cache["timestamp"],
                "from_cache": True
            }
        
        # 캐시된 데이터가 없으면 파일에서 가져오기
        image_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "images")
        
        # 디렉토리가 없으면 생성
        if not os.path.exists(image_dir):
            os.makedirs(image_dir, exist_ok=True)
            return None
        
        # class_distribution으로 시작하는 파일 찾기
        chart_files = [f for f in os.listdir(image_dir) if f.startswith('class_distribution_')]
        
        if not chart_files:
            return None
        
        # 파일명 기준으로 정렬하여 가장 최근 파일 찾기 (파일명에 날짜가 포함되어 있음)
        latest_file = sorted(chart_files, reverse=True)[0]
        
        # chart_와 table_ 파일도 찾기
        base_name = latest_file.split('.')[0]
        chart_file = f"{base_name}_chart.png"
        table_file = f"{base_name}_table.png"
        
        # 인구수 데이터 다시 생성
        # 캐시 업데이트 필요 (필요한 경우 직업별 인구수 차트를 다시 생성)
        logger.info(f"직업별 인구수 차트 이미지 파일 조회: {latest_file}")
        
        # 차트 이미지는 있지만 캐시 데이터가 없는 경우 빈 데이터 반환
        return {
            "success": True,
            "message": "직업별 인구 통계 정보 가져오기 성공 (캐시 없음)",
            "data": [],
            "total_population": 0,
            "imageUrl": f"/images/{latest_file}",
            "chartImageUrl": f"/images/{chart_file}" if os.path.exists(os.path.join(image_dir, chart_file)) else None,
            "tableImageUrl": f"/images/{table_file}" if os.path.exists(os.path.join(image_dir, table_file)) else None,
            "timestamp": datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST'),
            "from_cache": False
        }
    except Exception as e:
        logger.error(f"최신 직업별 인구수 차트 데이터 조회 중 오류: {e}")
        return None


def update_class_population_cache():
    """
    직업별 인구 통계 차트를 생성하고 캐시합니다.
    1시간마다 주기적으로 호출되어 캐시를 갱신합니다.
    
    Returns:
        bool: 성공 여부
    """
    try:
        # 직업별 인구 통계 차트 생성
        filename, job_data = generate_class_pie_chart()
        
        if filename and job_data:
            # 현재 시간(KST)
            current_time = get_current_time()
            timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S KST')
            
            # 캐시 업데이트
            _class_population_cache["data"] = job_data
            _class_population_cache["timestamp"] = timestamp
            _class_population_cache["imageUrl"] = f"/images/{filename}"
            _class_population_cache["chartImageUrl"] = f"/images/{job_data['chart_filename']}"
            _class_population_cache["tableImageUrl"] = f"/images/{job_data['table_filename']}"
            _class_population_cache["total_population"] = job_data["total_population"]
            _class_population_cache["jobs"] = job_data["jobs"]
            _class_population_cache["last_updated"] = current_time
            _class_population_cache["cache_time"] = current_time
            
            logger.info(f"직업별 인구 통계 차트 캐시 업데이트 완료 ({timestamp})")
            return True
        else:
            logger.error("직업별 인구 통계 차트 생성 실패")
            return False
    except Exception as e:
        logger.error(f"직업별 인구 통계 차트 캐시 업데이트 중 오류: {e}")
        return False