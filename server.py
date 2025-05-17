# api_server.py

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from api.rankData import rank_data
import logging
import sys
from service.background_tasks import start_background_tasks
from service.db_session import engine
import os
from datetime import datetime
from service.population_statistics import update_population_statistics, generate_class_pie_chart, get_latest_class_chart
from fastapi.responses import FileResponse, JSONResponse

# Configure logging
import sys
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# 루트 로거 설정
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# 모든 하위 모듈 로거가 메시지를 올바르게 전파하도록 설정
for log_name in ['api', 'service']:
    module_logger = logging.getLogger(log_name)
    module_logger.setLevel(logging.INFO)
    module_logger.propagate = True
    
    # 핸들러가 없으면 추가
    if not module_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        module_logger.addHandler(handler)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# IP Whitelist middleware
class IPWhitelistMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
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
                content={"message": "Access denied. Your IP is not whitelisted."}
            )
            
        # If IP is whitelisted, proceed with the request
        return await call_next(request)

app = FastAPI(title="MabiRank API")

# Add the IP whitelist middleware
app.add_middleware(IPWhitelistMiddleware)

# Mount static files directory for images
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")
os.makedirs(static_dir, exist_ok=True)
app.mount("/images", StaticFiles(directory=static_dir), name="images")

class SearchReq(BaseModel):
    server: str
    character: str

@app.post("/search", summary="캐릭터 랭킹 조회")
def api_search(req: SearchReq):
    # logger.info(f"--- 캐릭터 랭킹 조회시작: {req.server} - {req.character}")
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

# 인구 통계 강제 실행 엔드포인트
@app.get("/force-population-stats", summary="인구 통계 강제 저장")
def force_population_statistics():
    try:
        start_time = datetime.now()
        result = update_population_statistics()
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if result:
            logger.info(f"인구 통계 강제 실행 성공. 실행 시간: {execution_time}초")
            return {
                "success": True,
                "message": "인구 통계 저장 성공",
                "execution_time": f"{execution_time:.2f}초",
                "executed_at": start_time.strftime("%Y-%m-%d %H:%M:%S")
            }
        else:
            logger.error("인구 통계 강제 실행 실패")
            return {
                "success": False,
                "message": "인구 통계 저장 실패. 서버 로그를 확인해주세요."
            }
    except Exception as e:
        logger.error(f"인구 통계 강제 실행 엔드포인트 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 에러: {str(e)}")


# 직업별 인구 통계 파이차트 강제 생성 엔드포인트
@app.get("/force-class-chart", summary="직업별 인구 통계 파이차트 강제 생성")
def force_class_chart():
    try:
        start_time = datetime.now()
        filename, job_data = generate_class_pie_chart()
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if filename and job_data:
            logger.info(f"직업별 인구 통계 파이차트 생성 성공. 실행 시간: {execution_time}초, 파일명: {filename}")
            
            # JSON 형식으로 응답
            response_data = {
                "success": True,
                "message": "직업별 인구 통계 정보 가져오기 성공",
                "data": job_data["jobs"],  # 직업별 인구수 데이터
                "total_population": job_data["total_population"],  # 전체 인구수
                "imageUrl": f"/images/{filename}",  # 전체 이미지 URL
                "chartImageUrl": f"/images/{job_data['chart_filename']}",  # 차트 이미지 URL
                "tableImageUrl": f"/images/{job_data['table_filename']}",  # 테이블 이미지 URL
                "timestamp": job_data["timestamp"],  # 타임스태프
                "execution_time": f"{execution_time:.2f}초",  # 실행 시간
                "executed_at": start_time.strftime("%Y-%m-%d %H:%M:%S KST"),  # 실행 시간
                "from_cache": False  # 캠시 여부
            }
            
            return response_data
        else:
            logger.error("직업별 인구 통계 파이차트 생성 실패")
            return {
                "success": False,
                "message": "직업별 인구 통계 파이차트 생성 실패. 서버 로그를 확인해주세요."
            }
    except Exception as e:
        logger.error(f"직업별 인구 통계 파이차트 생성 엔드포인트 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")


# 최신 직업별 인구 통계 파이차트 조회 엔드포인트
@app.get("/class-chart", summary="최신 직업별 인구 통계 파이차트 조회")
def get_class_chart():
    try:
        # 최신 파이차트 파일명 가져오기
        filename = get_latest_class_chart()
        
        if not filename:
            # 파일이 없으면 새로 생성
            logger.info("최신 직업별 인구 통계 파이차트가 없어 새로 생성합니다.")
            filename = generate_class_pie_chart()
            
            if not filename:
                logger.error("직업별 인구 통계 파이차트 생성 실패")
                return JSONResponse(
                    status_code=404,
                    content={
                        "success": False,
                        "message": "직업별 인구 통계 파이차트를 찾을 수 없으며, 생성에도 실패했습니다."
                    }
                )
        
        # 이미지 파일 경로
        image_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "images",
            filename
        )
        
        if not os.path.exists(image_path):
            logger.error(f"파일을 찾을 수 없음: {image_path}")
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "message": "직업별 인구 통계 파이차트 파일을 찾을 수 없습니다."
                }
            )
        
        # 이미지 파일 반환
        return FileResponse(
            image_path,
            media_type="image/png",
            filename=filename
        )
    except Exception as e:
        logger.error(f"직업별 인구 통계 파이차트 조회 엔드포인트 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 에러: {str(e)}")



# Start background tasks when the server starts
@app.on_event("startup")
def startup_event():
    logger.info("Starting FastAPI server")
    # start_background_tasks()
    logger.info("Background tasks started")
