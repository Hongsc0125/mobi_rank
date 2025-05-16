import logging
from datetime import datetime
from sqlalchemy import text
from service.db_session import SessionLocal, KST

logger = logging.getLogger(__name__)

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
        GROUP BY server_name, class_name;
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