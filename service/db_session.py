from sqlalchemy import create_engine, types
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from pytz import timezone

# 한국 표준시(KST) 타임존 설정
KST = timezone('Asia/Seoul')

# 타임존 지원 DateTime 타입 정의
class KSTDateTime(types.TypeDecorator):
    impl = types.DateTime
    cache_ok = True
    
    def process_bind_param(self, value, dialect):
        if value is not None:
            if not value.tzinfo:
                raise ValueError("Timezone-aware datetime required")
            return value.astimezone(KST)
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return KST.localize(value)
        return None

# 데이터베이스 엔진 생성
engine = create_engine(
    "postgresql://super:Wkwkd119%21%21@207.180.212.248:5444/rank_data",
    pool_pre_ping=True,  # 연결 유효성 검사
    echo=False,  # SQL 쿼리 로깅 비활성화
)

# 세션 팩토리 생성
SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine,
    # 모든 datetime 값을 KST로 변환하는 타임존 설정
    timezone=KST
)

def get_db():
    """DB 세션을 반환하는 함수"""
    db = SessionLocal()
    try:
        # 세션의 타임존을 명시적으로 KST로 설정
        db.execute('SET timezone TO "Asia/Seoul"')
        yield db
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()

def get_current_time():
    """현재 시간을 KST 타임존으로 반환하는 함수"""
    return datetime.now(KST)
