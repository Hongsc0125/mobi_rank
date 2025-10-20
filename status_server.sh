#!/bin/bash

# Mabinogi Mobile 랭킹 서버 상태 확인 스크립트

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 설정
PROJECT_DIR="/root/mobi_rank"
PID_FILE="$PROJECT_DIR/server.pid"
LOG_FILE="$PROJECT_DIR/server.log"

echo -e "${BLUE}=== Mabinogi Mobile 랭킹 서버 상태 ===${NC}"
echo ""

# PID 파일 확인
if [ ! -f "$PID_FILE" ]; then
    echo -e "${RED}❌ 서버가 실행 중이지 않습니다${NC}"
    exit 1
fi

PID=$(cat "$PID_FILE")

# 프로세스 확인
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${RED}❌ PID $PID 프로세스가 실행 중이지 않습니다${NC}"
    rm -f "$PID_FILE"
    exit 1
fi

# 프로세스 정보
echo -e "${GREEN}✅ 서버 실행 중${NC}"
echo -e "   PID: $PID"
echo ""

# 메모리 사용량
MEM_INFO=$(ps -p "$PID" -o rss,vsz,pmem,pcpu --no-headers)
echo -e "${BLUE}리소스 사용량:${NC}"
echo "$MEM_INFO" | awk '{printf "   메모리: RSS=%d MB, VSZ=%d MB, %%MEM=%.1f%%, %%CPU=%.1f%%\n", $1/1024, $2/1024, $3, $4}'
echo ""

# 실행 시간
START_TIME=$(ps -p "$PID" -o lstart --no-headers)
echo -e "${BLUE}실행 시간:${NC}"
echo -e "   시작: $START_TIME"
ELAPSED=$(ps -p "$PID" -o etime --no-headers | tr -d ' ')
echo -e "   경과: $ELAPSED"
echo ""

# 포트 확인
PORT_INFO=$(netstat -tlnp 2>/dev/null | grep "$PID" | grep "8000" || ss -tlnp 2>/dev/null | grep "$PID" | grep "8000" || echo "")
if [ -n "$PORT_INFO" ]; then
    echo -e "${GREEN}✅ 포트 8000 LISTENING${NC}"
else
    echo -e "${YELLOW}⚠️  포트 8000을 찾을 수 없습니다${NC}"
fi
echo ""

# Chrome 프로세스 확인
CHROME_COUNT=$(pgrep -c chrome 2>/dev/null || echo "0")
CHROMEDRIVER_COUNT=$(pgrep -c chromedriver 2>/dev/null || echo "0")
echo -e "${BLUE}Chrome 프로세스:${NC}"
echo -e "   Chrome: $CHROME_COUNT 개"
echo -e "   ChromeDriver: $CHROMEDRIVER_COUNT 개"
echo ""

# 최근 로그 (에러 확인)
if [ -f "$LOG_FILE" ]; then
    ERROR_COUNT=$(grep -c "ERROR" "$LOG_FILE" 2>/dev/null || echo "0")
    WARNING_COUNT=$(grep -c "WARNING" "$LOG_FILE" 2>/dev/null || echo "0")

    echo -e "${BLUE}로그 상태:${NC}"
    echo -e "   에러: $ERROR_COUNT 개"
    echo -e "   경고: $WARNING_COUNT 개"
    echo -e "   파일: $LOG_FILE"
    echo ""

    if [ "$ERROR_COUNT" -gt 0 ]; then
        echo -e "${RED}최근 에러 (최대 5개):${NC}"
        grep "ERROR" "$LOG_FILE" | tail -5
        echo ""
    fi
fi

# API 상태 확인
echo -e "${BLUE}API 상태 확인:${NC}"
if command -v curl &> /dev/null; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/docs 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" = "200" ]; then
        echo -e "   ${GREEN}✅ API 정상 응답 (HTTP $HTTP_CODE)${NC}"
        echo -e "   문서: http://localhost:8000/docs"
    else
        echo -e "   ${YELLOW}⚠️  API 응답 없음 (HTTP $HTTP_CODE)${NC}"
    fi
else
    echo -e "   ${YELLOW}⚠️  curl이 설치되지 않아 확인할 수 없습니다${NC}"
fi
echo ""

echo -e "${BLUE}명령어:${NC}"
echo -e "   로그 보기: tail -f $LOG_FILE"
echo -e "   서버 중지: ./stop_server.sh"
echo -e "   서버 재시작: ./restart_server.sh"
