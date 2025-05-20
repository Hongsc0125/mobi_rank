from sqlalchemy import create_engine, types, text, event
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime
from pytz import timezone
import logging

logger = logging.getLogger(__name__)

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

# 데이터베이스 연결 문자열
SQLALCHEMY_DATABASE_URL = "postgresql://super:Wkwkd119%21%21@207.180.212.248:5444/rank_data"

# 데이터베이스 엔진 생성
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_pre_ping=True,  # 연결 유효성 검사
    echo=False,  # SQL 쿼리 로깅 비활성화
    pool_size=8,  # DB 쓰레드(연결 풀) 수 설정
    max_overflow=2  # 최대 초과 연결 수
)

# 세션 팩토리 생성
SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine
)

# 스레드 로컬 세션 (스레드 안전)
ScopedSession = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

# 세션이 시작될 때 타임존 설정
@event.listens_for(SessionLocal, 'after_begin')
def set_timezone(session, transaction, connection):
    connection.execute(text('SET TIME ZONE \'Asia/Seoul\''))

def get_db():
    """DB 세션을 반환하는 함수"""
    db = SessionLocal()
    try:
        # 세션의 타임존을 명시적으로 KST로 설정
        db.execute(text('SET TIME ZONE \'Asia/Seoul\''))
        yield db
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()

def get_current_time():
    """현재 시간을 KST 타임존으로 반환하는 함수"""
    return datetime.now(KST)
