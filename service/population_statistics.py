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




def update_population_statistics():
    """
    인구수 통계 데이터를 수집하고 저장하는 메인 함수
    mabinogi_ranking 테이블에서 각 랭킹 타입별(div 값에 따라 전투력, 매력, 생활력)로 데이터를 수집합니다.
    """
    try:
        # 1. 서버별 전체 인구수 수집 및 저장 (기존 로직 유지)
        server_data = get_all_server_populations()

        
        return True
    except Exception as e:
        logger.error(f"인구수 통계 업데이트 중 오류 발생: {e}")
        return False
