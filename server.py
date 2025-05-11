# api_server.py

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
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
        # 이미지 경로는 모든 IP 허용
        if request.url.path.startswith("/images"):
            return await call_next(request)
        
        # 다른 모든 경로는 IP 화이트리스트 검사
        # Get client's IP address
        client_ip = request.client.host
        
        # Define whitelist
        whitelist = [
            "207.180.212.248", 
            "127.0.0.1", 
            "localhost", 
            "59.12.47.180", 
            "2a02:c207:2258:5705::1",
            "218.233.5.245"
        ]
        
        # Check if client IP is in whitelist
        if client_ip not in whitelist:
            logger.warning(f"Blocked request from unauthorized IP: {client_ip}")
            return JSONResponse(
                status_code=403,
                content={"message": "접근이 거부되었습니다. 허용된 IP가 아닙니다."}
            )
            
        # If IP is whitelisted, proceed with the request
        return await call_next(request)

# 메인 애플리케이션 생성
app = FastAPI(title="MabiRank API")

# 이미지용 별도 앱 생성 (미들웨어 없음)
static_app = FastAPI()
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")
os.makedirs(static_dir, exist_ok=True)

# 메인 앱에 이미지 앱 마운트
app.mount("/images", StaticFiles(directory=static_dir), name="images")

# 미들웨어는 이미지 마운트 후에 추가 (이미지 경로에는 영향 없음)
app.add_middleware(IPWhitelistMiddleware)

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
