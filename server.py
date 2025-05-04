# api_server.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from crawler import search_character

app = FastAPI(title="MabiRank API")

class SearchReq(BaseModel):
    server: str
    character: str

@app.post("/search", summary="캐릭터 랭킹 조회")
def api_search(req: SearchReq):
    try:
        data = search_character(req.server, req.character)
        return {"success": True, "data": data}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="서버 에러: "+str(e))
