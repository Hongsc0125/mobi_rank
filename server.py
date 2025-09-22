# api_server.py

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager
from api.rankData import rank_data
import logging
import sys
from service.background_tasks import start_background_tasks
from service.db_session import engine
import os
from datetime import datetime
from service.population_statistics import update_population_statistics, generate_class_pie_chart, get_latest_class_chart
from service.html_image_converter import html_to_image
from fastapi.responses import FileResponse

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    start_background_tasks()
    logger.info("Background tasks started")

    # 검색 큐 시스템 초기화
    import threading
    from service.search_queue import create_search_queue_table
    from service.search_worker import search_worker_manager
    from service.persistent_ranking_cache import initialize_ranking_cache

    def init_systems():
        try:
            # 1. 검색 큐 테이블 생성
            logger.info("검색 큐 시스템 초기화 시작...")
            create_search_queue_table()
            logger.info("검색 큐 테이블 생성 완료")

            # 2. 검색 워커 시작 (1개 워커)
            search_worker_manager.start_workers(worker_count=1)
            logger.info("검색 워커 시작 완료")

            # 3. 고속 랭킹 캐시 초기화
            logger.info("고속 랭킹 캐시 초기화 시작...")
            success = initialize_ranking_cache()
            if success:
                logger.info("고속 랭킹 캐시 초기화 성공")
            else:
                logger.warning("고속 랭킹 캐시 초기화 실패, 기존 방식 사용")

        except Exception as e:
            logger.error(f"시스템 초기화 중 오류: {e}", exc_info=True)

    init_thread = threading.Thread(target=init_systems, daemon=True)
    init_thread.start()

    yield

    # Shutdown
    logger.info("서버 종료 중...")

    # 검색 워커 정리
    try:
        from service.search_worker import search_worker_manager
        search_worker_manager.stop_workers()
        logger.info("검색 워커 정리 완료")
    except Exception as e:
        logger.error(f"검색 워커 정리 중 오류: {e}")

    # 고속 랭킹 캐시 정리
    try:
        from service.persistent_ranking_cache import shutdown_ranking_cache
        shutdown_ranking_cache()
        logger.info("고속 랭킹 캐시 정리 완료")
    except Exception as e:
        logger.error(f"고속 랭킹 캐시 정리 중 오류: {e}")

app = FastAPI(title="MabiRank API", lifespan=lifespan)

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
def api_search(req: SearchReq, request: Request):
    """
    캐릭터 랭킹 조회 요청을 큐에 추가하고 작업 ID를 반환합니다.
    반환된 작업 ID로 /search/status/{job_id}를 호출하여 결과를 확인할 수 있습니다.
    """
    try:
        from service.search_queue import search_queue_manager
        
        # 클라이언트 정보 추출
        client_ip = request.client.host
        user_agent = request.headers.get("user-agent", "")
        
        # 검색 요청을 큐에 추가
        request_id = search_queue_manager.enqueue_search_request(
            server=req.server,
            character_name=req.character,
            client_ip=client_ip,
            user_agent=user_agent,
            priority=10  # 일반 우선순위
        )
        
        # 작업 ID 반환
        return {
            "success": True,
            "job_id": request_id,
            "message": "검색 요청이 큐에 추가되었습니다.",
            "status_url": f"/search/status/{request_id}",
            "estimated_wait_time": "약 10-30초 (큐 상황에 따라 변동)"
        }
        
    except Exception as e:
        logger.error(f"검색 요청 큐 추가 실패: {e}")
        raise HTTPException(status_code=500, detail=f"서버 에러: {str(e)}")

@app.get("/search/status/{job_id}", summary="검색 작업 상태 및 결과 조회")
def get_search_status(job_id: str):
    """
    검색 작업의 상태와 결과를 조회합니다.
    작업이 완료된 경우 기존 /search API와 동일한 형식으로 결과를 반환합니다.
    """
    try:
        from service.search_queue import search_queue_manager
        
        # 요청 상태 조회
        status_info = search_queue_manager.get_request_status(job_id)
        
        if not status_info:
            raise HTTPException(status_code=404, detail="검색 작업을 찾을 수 없습니다.")
        
        # 상태별 응답 구성
        if status_info["status"] == "completed" and status_info["result"]:
            # 완료된 경우: 기존 /search API와 동일한 형식으로 반환
            result = status_info["result"]
            
            if isinstance(result, dict) and "data" in result and "success" in result:
                character_data = result.get("data")
                message = result.get("message", "")
                success = result.get("success", False)
                
                response = {
                    "success": success,
                    "message": message,
                    "from_cache": result.get("from_cache", False),
                    "job_id": job_id,
                    "status": "completed",
                    "completed_at": status_info["completed_at"]
                }
                
                if character_data:
                    response["character"] = character_data
                    
                return response
            else:
                # 예상과 다른 결과 형식인 경우
                return {
                    "success": True,
                    "job_id": job_id,
                    "status": "completed",
                    "result": result,
                    "completed_at": status_info["completed_at"]
                }
            
        elif status_info["status"] == "failed":
            # 실패한 경우
            return {
                "success": False,
                "job_id": job_id,
                "status": "failed",
                "error": status_info["error_message"],
                "retry_count": status_info["retry_count"],
                "message": "검색이 실패했습니다.",
                "server": status_info["server"],
                "character_name": status_info["character_name"]
            }
            
        elif status_info["status"] == "processing":
            # 처리 중인 경우
            return {
                "success": True,
                "job_id": job_id,
                "status": "processing",
                "message": "검색이 처리 중입니다.",
                "server": status_info["server"],
                "character_name": status_info["character_name"],
                "started_at": status_info["started_at"]
            }
            
        elif status_info["status"] == "pending":
            # 대기 중인 경우
            return {
                "success": True,
                "job_id": job_id,
                "status": "pending",
                "message": "검색이 대기 중입니다.",
                "server": status_info["server"],
                "character_name": status_info["character_name"],
                "created_at": status_info["created_at"]
            }
            
        else:
            # 기타 상태 (timeout 등)
            return {
                "success": False,
                "job_id": job_id,
                "status": status_info["status"],
                "message": f"검색 상태: {status_info['status']}",
                "error": status_info.get("error_message"),
                "server": status_info["server"],
                "character_name": status_info["character_name"]
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"검색 상태 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"서버 에러: {str(e)}")

@app.post("/search/async", summary="캐릭터 랭킹 조회 (비동기)")
def api_search_async(req: SearchReq, request: Request):
    """
    비동기 방식의 캐릭터 랭킹 조회입니다.
    요청 ID를 즉시 반환하고, /search/status/{request_id}로 결과를 확인할 수 있습니다.
    """
    try:
        from service.search_queue import search_queue_manager
        
        # 클라이언트 정보 추출
        client_ip = request.client.host
        user_agent = request.headers.get("user-agent", "")
        
        # 검색 요청을 큐에 추가
        request_id = search_queue_manager.enqueue_search_request(
            server=req.server,
            character_name=req.character,
            client_ip=client_ip,
            user_agent=user_agent,
            priority=10  # 일반 우선순위
        )
        
        # 요청 ID 반환 (비동기 처리)
        return {
            "success": True,
            "request_id": request_id,
            "message": "검색 요청이 큐에 추가되었습니다. /search/status/{request_id}로 상태를 확인하세요.",
            "status_url": f"/search/status/{request_id}"
        }
        
    except Exception as e:
        logger.error(f"비동기 검색 요청 실패: {e}")
        raise HTTPException(status_code=500, detail=f"서버 에러: {str(e)}")

@app.post("/search/sync", summary="캐릭터 랭킹 조회 (동기식)")
def api_search_sync(req: SearchReq):
    """
    기존 방식의 동기식 캐릭터 랭킹 조회입니다.
    큐 시스템을 거치지 않고 즉시 처리하여 결과를 반환합니다.
    """
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
                "from_cache": result.get("from_cache", False),
                "queue_processed": False
            }
            
            # Only include character data if it exists
            if character_data:
                response["character"] = character_data
                
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

# 랭킹 캐시 상태 확인 엔드포인트
@app.get("/cache-status", summary="고속 랭킹 캐시 상태 확인")
def get_cache_status():
    try:
        from service.fast_ranking_service import get_cache_status
        status = get_cache_status()
        return {
            "success": True,
            "cache_status": status,
            "message": "캐시 상태 조회 성공"
        }
    except Exception as e:
        logger.error(f"캐시 상태 조회 중 오류: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "캐시 상태 조회 실패"
        }

# 검색 큐 관리 엔드포인트들
@app.get("/search/queue/status", summary="검색 큐 상태 조회")
def get_queue_status():
    """검색 큐와 워커의 상태를 조회합니다."""
    try:
        from service.search_worker import search_worker_manager
        from service.search_queue import search_queue_manager
        
        worker_status = search_worker_manager.get_status()
        queue_stats = search_queue_manager.get_queue_stats()
        
        return {
            "success": True,
            "worker_manager": worker_status,
            "queue_statistics": queue_stats,
            "message": "큐 상태 조회 성공"
        }
    except Exception as e:
        logger.error(f"큐 상태 조회 중 오류: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "큐 상태 조회 실패"
        }

@app.post("/search/queue/cleanup", summary="오래된 검색 요청 정리")
def cleanup_old_requests():
    """완료된 오래된 검색 요청들을 정리합니다."""
    try:
        from service.search_queue import search_queue_manager
        
        search_queue_manager.cleanup_old_requests(days=1)
        
        return {
            "success": True,
            "message": "오래된 검색 요청 정리 완료"
        }
    except Exception as e:
        logger.error(f"검색 요청 정리 중 오류: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "검색 요청 정리 실패"
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

