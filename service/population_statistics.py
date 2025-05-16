import logging
from datetime import datetime
from sqlalchemy import func
from service.db_session import SessionLocal, KST
from models import MabinogiPopulationStatistics, MabinogiRanking

logger = logging.getLogger(__name__)

def get_population_statistics(session):
    today = datetime.now(KST).date()
    now_kst = datetime.now(KST)

    # 1. 전체 인구수
    total_population = session.query(func.count(MabinogiRanking.id)).scalar()
    yield MabinogiPopulationStatistics(
        date=today,
        server_name='all',
        class_name='all',
        population=total_population,
        retrieved_at=now_kst
    )

    # 2. 전체 직업별 인구수
    class_counts = (
        session.query(
            MabinogiRanking.class_name,
            func.count(MabinogiRanking.id).label("population")
        )
        .group_by(MabinogiRanking.class_name)
        .all()
    )
    for row in class_counts:
        yield MabinogiPopulationStatistics(
            date=today,
            server_name='all',
            class_name=row.class_name,
            population=row.population,
            retrieved_at=now_kst
        )

    # 3. 서버별 전체 인구수
    server_counts = (
        session.query(
            MabinogiRanking.server_name,
            func.count(MabinogiRanking.id).label("population")
        )
        .group_by(MabinogiRanking.server_name)
        .all()
    )
    for row in server_counts:
        yield MabinogiPopulationStatistics(
            date=today,
            server_name=row.server_name,
            class_name='all',
            population=row.population,
            retrieved_at=now_kst
        )

    # 4. 서버별 직업별 인구수
    server_class_counts = (
        session.query(
            MabinogiRanking.server_name,
            MabinogiRanking.class_name,
            func.count(MabinogiRanking.id).label("population")
        )
        .group_by(MabinogiRanking.server_name, MabinogiRanking.class_name)
        .all()
    )
    for row in server_class_counts:
        yield MabinogiPopulationStatistics(
            date=today,
            server_name=row.server_name,
            class_name=row.class_name,
            population=row.population,
            retrieved_at=now_kst
        )

def update_population_statistics():
    """
    매일 00시에 전체/직업별/서버별/서버별-직업별 인구수를 통계 테이블에 저장합니다.
    """
    session = SessionLocal()
    try:
        stats = list(get_population_statistics(session))
        session.bulk_save_objects(stats)
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"인구수 통계 업데이트 중 오류 발생: {e}")
        return False
    finally:
        session.close()