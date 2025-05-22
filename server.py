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
from service.html_image_converter import html_to_image
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
    
class HtmlImageReq(BaseModel):
    html: str

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
@app.get("/force_class_chart", summary="직업별 인구 통계 파이차트 강제 생성")
def force_class_chart():
    try:
        start_time = datetime.now()
        # 직업별 인구 통계 캐시 업데이트
        from service.population_statistics import update_class_population_cache
        result = update_class_population_cache()
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if result:
            # 캐시에서 최신 데이터 가져오기
            from service.population_statistics import _class_population_cache as cache
            logger.info(f"직업별 인구 통계 파이차트 생성 성공. 실행 시간: {execution_time}초")
            
            # JSON 형식으로 응답
            response_data = {
                "success": True,
                "message": "직업별 인구 통계 정보 가져오기 성공",
                "data": cache["jobs"],  # 직업별 인구수 데이터
                "total_population": cache["total_population"],  # 전체 인구수
                "imageUrl": cache["imageUrl"],  # 전체 이미지 URL
                "chartImageUrl": cache["chartImageUrl"],  # 차트 이미지 URL
                "tableImageUrl": cache["tableImageUrl"],  # 테이블 이미지 URL
                "timestamp": cache["timestamp"],  # 타임스태프
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
@app.get("/class_chart", summary="최신 직업별 인구 통계 파이차트 조회")
def get_class_chart():
    try:
        # 캐시된 데이터 가져오기
        from service.population_statistics import get_latest_class_chart
        result = get_latest_class_chart()
        
        if not result:
            logger.warning("직업별 인구 통계 파이차트 데이터가 없습니다.")
            return {
                "success": False,
                "message": "직업별 인구 통계 데이터가 없습니다.",
                "imageUrl": None
            }
            
        # 직접 직업별 인구 통계 정보 가져오기 성공
        logger.info(f"직업별 인구 통계 파이차트 조회 성공. 캐시: {result.get('from_cache', False)}")
        
        return result
            
    except Exception as e:
        logger.error(f"직업별 인구 통계 파이차트 조회 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 오류: {str(e)}")


# HTML을 이미지로 변환하는 엔드포인트
@app.post("/html_to_image", summary="HTML 테이블을 이미지로 변환")
def convert_html_to_image(req: HtmlImageReq):
    try:
        # HTML 검증 및 전처리 (빈 셀 처리)
        processed_html = req.html.replace("<td></td>", "<td>&nbsp;</td>")
        
        # HTML을 이미지로 변환
        result = html_to_image(processed_html)
        
        if result["success"]:
            return {
                "success": True,
                "imageUrl": result["imageUrl"],
                "message": "HTML 테이블 이미지 변환 성공"
            }
        else:
            raise HTTPException(status_code=500, detail="HTML 테이블 이미지 변환 실패")
    
    except Exception as e:
        logger.error(f"HTML 테이블 이미지 변환 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=500, detail=f"HTML 테이블 이미지 변환 오류: {str(e)}")

# Start background tasks when the server starts
@app.on_event("startup")
async def startup_event():
    start_background_tasks()
    logger.info("Background tasks started")
