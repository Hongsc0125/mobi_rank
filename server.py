# api_server.py

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from api.rankData import rank_data
import logging
from service.background_tasks import start_background_tasks
from service.db_session import engine
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# IP Whitelist middleware
class IPWhitelistMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        request_path = request.url.path
        
        # 이미지 경로인지 확인 (정확한 경로 패턴 처리)
        if request_path.startswith("/images"):
            logger.info(f"[트래픽] 이미지 액세스 허용: {request_path} (IP: {client_ip})")
            return await call_next(request)
        
        # IP 화이트리스트 정의
        whitelist = [
            "207.180.212.248", 
            "127.0.0.1", 
            "localhost", 
            "59.12.47.180", 
            "2a02:c207:2258:5705::1",
            "218.233.5.245"
        ]
        
        # IP 화이트리스트 검사
        if client_ip not in whitelist:
            logger.warning(f"[접근 거부] 미승인 IP: {client_ip}, 경로: {request_path}")
            return JSONResponse(
                status_code=403,
                content={"message": "접근이 거부되었습니다. 허용된 IP가 아닙니다."}
            )
        
        logger.info(f"[트래픽] 일반 액세스 허용: {request_path} (IP: {client_ip})")
        return await call_next(request)

# 메인 애플리케이션 생성
app = FastAPI(title="MabiRank API")

# 이미지 디렉토리 설정
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")
os.makedirs(static_dir, exist_ok=True)

# 이미지용 욕시 이미지 디렉토리만 인스턴스화
# 이 인스턴스는 미들웨어를 적용하지 않는 완전히 별도의 액세스 포인트
image_app = FastAPI()
image_app.mount("/", StaticFiles(directory=static_dir), name="images")

# 메인 앱에 IP 화이트리스트 미들웨어 추가
app.add_middleware(IPWhitelistMiddleware)

# 이미지 액세스를 위한 별도 라우트 설정 - 메인 앱과는 별도
@app.get("/images/{path:path}", include_in_schema=False)
async def serve_images(path: str):
    # 여기서는 어떤 IP 체크도 하지 않음
    file_path = os.path.join(static_dir, path)
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return FileResponse(file_path)
    return JSONResponse(status_code=404, content={"message": "파일을 찾을 수 없습니다."})

# 원래 마운트는 사용하지 않음 - 가능하면 주석 처리 부탁
# app.mount("/images", StaticFiles(directory=static_dir), name="images")

class SearchReq(BaseModel):
    server: str
    character: str

@app.post("/search", summary="캐릭터 랭킹 조회")
def api_search(req: SearchReq):
    try:
        result = rank_data(req.server, req.character)
        
        # Flatten the response structure to avoid nested data fields
        if isinstance(result, dict) and "data" in result and "success" in result:
            # Extract the relevant fields
            character_data = result.get("data")
            message = result.get("message", "")
            success = result.get("success", False)
            
            # Return a flattened response
            response = {
                "success": success,
                "message": message,
                "from_cache": result.get("from_cache", False)
            }
            
            # Only include character data if it exists
            if character_data:
                response["character"] = character_data

            logger.info(f"Response: {response}")
                
            return response
        
        # If result structure is unexpected, return as is
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="서버 에러: "+str(e))

# Population data endpoint
@app.get("/population", summary="서버별 인구수 조회")
def get_population():
    try:
        from service.population import get_population_data
        result = get_population_data()
        return result
    except Exception as e:
        logger.error(f"Population endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"서버 에러: {str(e)}")

# Start background tasks when the server starts
@app.on_event("startup")
def startup_event():
    logger.info("Starting FastAPI server")
    # Start background tasks
    start_background_tasks()
    logger.info("Background tasks started")
