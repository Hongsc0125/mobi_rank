# 마비노기 모바일 랭킹 API 사용 가이드

## 🚀 개요
마비노기 모바일 캐릭터 랭킹 조회 API는 큐 기반 비동기 처리 시스템을 사용합니다.
모든 검색 요청은 큐에 추가되어 순차적으로 처리되며, 작업 ID를 통해 결과를 조회할 수 있습니다.

## 📝 Base URL
```
https://your-api-domain.com
```

## 🔍 캐릭터 랭킹 검색

### 1. 검색 요청 시작

**Endpoint:** `POST /search`

**Description:** 캐릭터 랭킹 검색을 큐에 추가하고 작업 ID를 반환합니다.

#### Request Body
```json
{
  "server": "서버명",
  "character": "캐릭터명"
}
```

#### 서버명 목록
- `데이안`
- `아이라` 
- `던컨`
- `알리사`
- `메이븐`
- `라사`
- `칼릭스`

#### Response (성공)
```json
{
  "success": true,
  "job_id": "uuid-형식-작업-id",
  "message": "검색 요청이 큐에 추가되었습니다.",
  "status_url": "/search/status/uuid-형식-작업-id",
  "estimated_wait_time": "약 10-30초 (큐 상황에 따라 변동)"
}
```

#### Example Request
```bash
curl -X POST "https://api.example.com/search" \
  -H "Content-Type: application/json" \
  -d '{
    "server": "던컨",
    "character": "은빛나리"
  }'
```

### 2. 결과 조회

**Endpoint:** `GET /search/status/{job_id}`

**Description:** 작업 상태를 확인하고, 완료된 경우 검색 결과를 반환합니다.

#### Response - 대기 중 (pending)
```json
{
  "success": true,
  "job_id": "uuid-형식-작업-id",
  "status": "pending",
  "message": "검색이 대기 중입니다.",
  "server": "던컨",
  "character_name": "은빛나리",
  "created_at": "2025-07-03T16:49:28+09:00"
}
```

#### Response - 처리 중 (processing)
```json
{
  "success": true,
  "job_id": "uuid-형식-작업-id",
  "status": "processing",
  "message": "검색이 처리 중입니다.",
  "server": "던컨",
  "character_name": "은빛나리",
  "started_at": "2025-07-03T16:49:30+09:00"
}
```

#### Response - 완료 (completed)
```json
{
  "success": true,
  "message": "Character found in rankings (DB update in progress)",
  "from_cache": false,
  "job_id": "uuid-형식-작업-id",
  "status": "completed",
  "completed_at": "2025-07-03T16:49:51+09:00",
  "character": {
    "character": "은빛나리",
    "server": "던컨",
    "retrieved_at": "2025-07-03 16:49:28",
    "rankings": {
      "전투력": {
        "rank": "70,469위",
        "change": 200554,
        "change_type": "up",
        "server": "던컨",
        "character": "은빛나리",
        "class": "힐러",
        "power": "24,202"
      },
      "매력": {
        "rank": "45,272위",
        "change": 251225,
        "change_type": "up",
        "server": "던컨",
        "character": "은빛나리",
        "class": "힐러",
        "power": "14,656"
      },
      "생활력": {
        "rank": "190,588위",
        "change": 39649,
        "change_type": "down",
        "server": "던컨",
        "character": "은빛나리",
        "class": "힐러",
        "power": "6,265"
      }
    }
  }
}
```

#### Response - 실패 (failed)
```json
{
  "success": false,
  "job_id": "uuid-형식-작업-id",
  "status": "failed",
  "error": "캐릭터 '존재하지않는캐릭터'을(를) 서버 '던컨'에서 찾을 수 없습니다.",
  "retry_count": 3,
  "message": "검색이 실패했습니다.",
  "server": "던컨",
  "character_name": "존재하지않는캐릭터"
}
```

## 💻 사용 예시

### JavaScript (Node.js)
```javascript
const axios = require('axios');

async function searchCharacter(server, character) {
  try {
    // 1. 검색 요청
    const searchResponse = await axios.post('https://api.example.com/search', {
      server: server,
      character: character
    });
    
    const jobId = searchResponse.data.job_id;
    console.log(`검색 작업 시작됨. Job ID: ${jobId}`);
    
    // 2. 결과 대기 (폴링)
    while (true) {
      await new Promise(resolve => setTimeout(resolve, 2000)); // 2초 대기
      
      const statusResponse = await axios.get(`https://api.example.com/search/status/${jobId}`);
      const status = statusResponse.data;
      
      console.log(`상태: ${status.status}`);
      
      if (status.status === 'completed') {
        console.log('검색 완료!', status.character);
        return status;
      } else if (status.status === 'failed') {
        console.error('검색 실패:', status.error);
        return null;
      }
      // pending 또는 processing인 경우 계속 대기
    }
  } catch (error) {
    console.error('API 요청 실패:', error.message);
    return null;
  }
}

// 사용 예시
searchCharacter('던컨', '은빛나리')
  .then(result => {
    if (result) {
      console.log('최종 결과:', result);
    }
  });
```

### Python
```python
import requests
import time
import json

def search_character(server, character):
    base_url = "https://api.example.com"
    
    try:
        # 1. 검색 요청
        search_response = requests.post(f"{base_url}/search", json={
            "server": server,
            "character": character
        })
        search_response.raise_for_status()
        
        job_id = search_response.json()["job_id"]
        print(f"검색 작업 시작됨. Job ID: {job_id}")
        
        # 2. 결과 대기 (폴링)
        while True:
            time.sleep(2)  # 2초 대기
            
            status_response = requests.get(f"{base_url}/search/status/{job_id}")
            status_response.raise_for_status()
            
            status = status_response.json()
            print(f"상태: {status['status']}")
            
            if status["status"] == "completed":
                print("검색 완료!")
                return status
            elif status["status"] == "failed":
                print(f"검색 실패: {status['error']}")
                return None
            # pending 또는 processing인 경우 계속 대기
            
    except requests.exceptions.RequestException as e:
        print(f"API 요청 실패: {e}")
        return None

# 사용 예시
result = search_character("던컨", "은빛나리")
if result:
    print("최종 결과:", json.dumps(result, indent=2, ensure_ascii=False))
```

### PHP
```php
<?php

function searchCharacter($server, $character) {
    $baseUrl = "https://api.example.com";
    
    // 1. 검색 요청
    $searchData = json_encode([
        "server" => $server,
        "character" => $character
    ]);
    
    $searchContext = stream_context_create([
        'http' => [
            'method' => 'POST',
            'header' => 'Content-Type: application/json',
            'content' => $searchData
        ]
    ]);
    
    $searchResponse = file_get_contents("$baseUrl/search", false, $searchContext);
    if ($searchResponse === FALSE) {
        echo "검색 요청 실패\n";
        return null;
    }
    
    $searchResult = json_decode($searchResponse, true);
    $jobId = $searchResult['job_id'];
    echo "검색 작업 시작됨. Job ID: $jobId\n";
    
    // 2. 결과 대기 (폴링)
    while (true) {
        sleep(2); // 2초 대기
        
        $statusResponse = file_get_contents("$baseUrl/search/status/$jobId");
        if ($statusResponse === FALSE) {
            echo "상태 조회 실패\n";
            return null;
        }
        
        $status = json_decode($statusResponse, true);
        echo "상태: " . $status['status'] . "\n";
        
        if ($status['status'] === 'completed') {
            echo "검색 완료!\n";
            return $status;
        } elseif ($status['status'] === 'failed') {
            echo "검색 실패: " . $status['error'] . "\n";
            return null;
        }
        // pending 또는 processing인 경우 계속 대기
    }
}

// 사용 예시
$result = searchCharacter("던컨", "은빛나리");
if ($result) {
    echo "최종 결과:\n";
    echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

?>
```

## 📊 추가 API 엔드포인트

### 큐 상태 조회
**Endpoint:** `GET /search/queue/status`

큐와 워커의 현재 상태를 조회합니다.

```json
{
  "success": true,
  "worker_manager": {
    "running": true,
    "worker_count": 1,
    "total_processed": 45,
    "total_failed": 2
  },
  "queue_statistics": {
    "pending": 3,
    "processing": 1,
    "completed": 42,
    "failed": 2,
    "timeout": 0
  }
}
```

## ⚠️ 주의사항

1. **처리 시간**: 검색 작업은 보통 10-30초 소요되며, 큐 상황에 따라 더 오래 걸릴 수 있습니다.

2. **폴링 간격**: 결과 조회 시 최소 1-2초 간격으로 요청하세요. 너무 빈번한 요청은 서버에 부하를 줄 수 있습니다.

3. **타임아웃**: 작업이 5분 이상 처리되지 않으면 자동으로 타임아웃 처리됩니다.

4. **재시도**: 실패한 작업은 최대 3회까지 자동 재시도됩니다.

5. **캐릭터명**: 정확한 캐릭터명을 입력해야 합니다. 대소문자를 구분합니다.

6. **서버명**: 지원되는 서버명만 사용 가능합니다.

## 🚦 HTTP 상태 코드

- `200`: 성공
- `400`: 잘못된 요청 (서버명, 캐릭터명 오류 등)
- `404`: 작업 ID를 찾을 수 없음
- `408`: 타임아웃
- `500`: 서버 내부 오류

## 💡 팁

1. **효율적인 폴링**: 처음에는 2초 간격으로 시작하여, 시간이 지나면 간격을 늘려 서버 부하를 줄이세요.

2. **에러 처리**: 네트워크 오류나 서버 오류에 대비한 재시도 로직을 구현하세요.

3. **캐시 활용**: `from_cache: true`인 경우 캐시된 데이터이므로 더 빠르게 응답됩니다.

4. **작업 저장**: `job_id`를 저장해두면 나중에 결과를 다시 조회할 수 있습니다.