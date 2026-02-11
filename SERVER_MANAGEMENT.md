# 서버 관리 가이드

## 📋 개요

Mabinogi Mobile 랭킹 서버를 백그라운드에서 실행하고 관리하는 스크립트 모음입니다.

## 🚀 사용법

### 서버 시작

```bash
./start_server.sh
```

**기능:**
- 기존 프로세스 자동 정리 (Chrome, ChromeDriver)
- 서버를 백그라운드(nohup)로 시작
- 초기화 완료 대기 (최대 30초)
- PID 파일 생성 (`server.pid`)
- 로그 파일 자동 백업 (10MB 이상 시)

**출력 예시:**
```
✅ 서버가 성공적으로 시작되었습니다!
   PID: 1998393
   로그: /root/mobi_rank/server.log
   URL: http://0.0.0.0:8000
```

---

### 서버 중지

```bash
./stop_server.sh
```

**기능:**
- Graceful shutdown 시도 (10초 대기)
- 응답 없으면 강제 종료 (SIGKILL)
- Chrome/ChromeDriver 프로세스 정리
- PID 파일 삭제

---

### 서버 재시작

```bash
./restart_server.sh
```

**기능:**
- `stop_server.sh` → 2초 대기 → `start_server.sh` 순차 실행

---

### 서버 상태 확인

```bash
./status_server.sh
```

**출력 정보:**
- ✅ 실행 상태 (PID)
- 📊 리소스 사용량 (메모리, CPU)
- ⏱️ 실행 시간
- 🔌 포트 상태 (8000)
- 🌐 Chrome 프로세스 개수
- 📝 로그 통계 (에러, 경고)
- 🔍 API 응답 상태
- 📋 최근 에러 (최대 5개)

**출력 예시:**
```
=== Mabinogi Mobile 랭킹 서버 상태 ===

✅ 서버 실행 중
   PID: 1998393

리소스 사용량:
   메모리: RSS=215 MB, VSZ=1843 MB, %MEM=1.7%, %CPU=40.6%

실행 시간:
   시작: Mon Oct 20 15:50:17 2025
   경과: 00:35

✅ 포트 8000 LISTENING

Chrome 프로세스:
   Chrome: 7 개
   ChromeDriver: 7 개

로그 상태:
   에러: 7 개
   경고: 7 개
   파일: /root/mobi_rank/server.log

✅ API 정상 응답 (HTTP 200)
   문서: http://localhost:8000/docs
```

---

## 📂 파일 구조

```
/root/mobi_rank/
├── start_server.sh      # 서버 시작 스크립트
├── stop_server.sh       # 서버 중지 스크립트
├── restart_server.sh    # 서버 재시작 스크립트
├── status_server.sh     # 서버 상태 확인 스크립트
├── server.py            # FastAPI 서버 메인 파일
├── server.pid           # 서버 PID (자동 생성)
└── server.log           # 서버 로그 (자동 생성)
```

---

## 📖 로그 확인

### 실시간 로그 보기

```bash
tail -f /root/mobi_rank/server.log
```

### 최근 100줄 보기

```bash
tail -100 /root/mobi_rank/server.log
```

### 에러만 필터링

```bash
grep "ERROR" /root/mobi_rank/server.log | tail -50
```

---

## 🔧 트러블슈팅

### 1. 서버가 시작되지 않음

```bash
# 로그 확인
tail -100 /root/mobi_rank/server.log

# 포트 8000 사용 중 확인
netstat -tlnp | grep 8000
# 또는
ss -tlnp | grep 8000
```

### 2. 프로세스가 좀비 상태

```bash
# 모든 관련 프로세스 강제 종료
killall -9 python chrome chromedriver

# PID 파일 수동 삭제
rm -f /root/mobi_rank/server.pid

# 서버 재시작
./start_server.sh
```

### 3. 로그 파일이 너무 큼

로그 파일은 10MB 이상일 때 자동으로 백업됩니다.
수동 백업:

```bash
mv /root/mobi_rank/server.log /root/mobi_rank/server.log.backup_$(date +%Y%m%d)
```

---

## 🔄 자동 시작 (systemd)

서버 재부팅 시 자동으로 시작하려면:

```bash
sudo nano /etc/systemd/system/mobi-rank.service
```

```ini
[Unit]
Description=Mabinogi Mobile Ranking Server
After=network.target

[Service]
Type=forking
User=root
WorkingDirectory=/root/mobi_rank
ExecStart=/root/mobi_rank/start_server.sh
ExecStop=/root/mobi_rank/stop_server.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

활성화:

```bash
sudo systemctl daemon-reload
sudo systemctl enable mobi-rank
sudo systemctl start mobi-rank
```

---

## 📊 API 엔드포인트

서버 실행 후 다음 URL에서 API 문서를 확인할 수 있습니다:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

---

## ⚡ 성능 모니터링

### CPU/메모리 사용량 실시간 모니터링

```bash
watch -n 1 './status_server.sh | grep -A 1 "리소스 사용량"'
```

### Chrome 프로세스 개수 모니터링

```bash
watch -n 5 'pgrep -c chrome; pgrep -c chromedriver'
```

---

## 🛡️ 보안 주의사항

- IP 화이트리스트가 설정되어 있습니다
- 로그 파일에 민감한 정보가 포함될 수 있으므로 주의하세요
- 정기적으로 로그 파일을 백업하고 삭제하세요

---

## 📞 문의

문제가 발생하면 로그 파일과 함께 이슈를 보고해주세요.
