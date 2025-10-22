#!/bin/bash

# Mabinogi Mobile 랭킹 서버 시작 스크립트

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 설정
PROJECT_DIR="/root/mobi_rank"
PID_FILE="$PROJECT_DIR/server.pid"
LOG_FILE="$PROJECT_DIR/server.log"
VENV_PATH="$PROJECT_DIR/.venv"
PYTHON_CMD="$VENV_PATH/bin/python"

# 현재 디렉토리로 이동
cd "$PROJECT_DIR" || exit 1

# 가상환경 확인
if [ ! -d "$VENV_PATH" ]; then
    echo -e "${RED}❌ 가상환경을 찾을 수 없습니다: $VENV_PATH${NC}"
    exit 1
fi

# 서버가 이미 실행 중인지 확인
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo -e "${YELLOW}서버가 이미 실행 중입니다 (PID: $PID)${NC}"
        echo -e "${YELLOW}중지하려면 ./stop_server.sh를 실행하세요${NC}"
        exit 1
    else
        echo -e "${YELLOW}PID 파일이 있지만 프로세스가 없습니다. 정리 중...${NC}"
        rm -f "$PID_FILE"
    fi
fi

# 기존 Chrome/ChromeDriver 프로세스 정리
echo -e "${GREEN}기존 프로세스 정리 중...${NC}"
killall -9 chrome chromedriver 2>/dev/null || true
sleep 2

# 로그 파일 백업 (크기가 10MB 이상이면)
if [ -f "$LOG_FILE" ]; then
    SIZE=$(stat -f%z "$LOG_FILE" 2>/dev/null || stat -c%s "$LOG_FILE" 2>/dev/null)
    if [ "$SIZE" -gt 10485760 ]; then
        echo -e "${GREEN}로그 파일 백업 중...${NC}"
        mv "$LOG_FILE" "$LOG_FILE.$(date +%Y%m%d_%H%M%S)"
    fi
fi

# 서버 시작
echo -e "${GREEN}서버 시작 중...${NC}"
nohup $PYTHON_CMD server.py > "$LOG_FILE" 2>&1 &
SERVER_PID=$!

# PID 저장
echo $SERVER_PID > "$PID_FILE"

# 서버 시작 확인 (최대 30초 대기)
echo -e "${GREEN}서버 초기화 대기 중...${NC}"
for i in {1..30}; do
    sleep 1
    if grep -q "Application startup complete" "$LOG_FILE" 2>/dev/null; then
        echo -e "${GREEN}✅ 서버가 성공적으로 시작되었습니다!${NC}"
        echo -e "${GREEN}   PID: $SERVER_PID${NC}"
        echo -e "${GREEN}   로그: $LOG_FILE${NC}"
        echo -e "${GREEN}   URL: http://0.0.0.0:8000${NC}"
        echo ""
        echo -e "${YELLOW}로그 확인: tail -f $LOG_FILE${NC}"
        echo -e "${YELLOW}서버 중지: ./stop_server.sh${NC}"
        echo -e "${YELLOW}서버 상태: ./status_server.sh${NC}"
        exit 0
    fi

    # 프로세스가 죽었는지 확인
    if ! ps -p $SERVER_PID > /dev/null 2>&1; then
        echo -e "${RED}❌ 서버 시작 실패!${NC}"
        echo -e "${RED}마지막 50줄 로그:${NC}"
        tail -50 "$LOG_FILE"
        rm -f "$PID_FILE"
        exit 1
    fi

    echo -n "."
done

echo ""
echo -e "${YELLOW}⚠️  서버가 시작되었지만 초기화 확인 타임아웃${NC}"
echo -e "${YELLOW}   로그를 확인하세요: tail -f $LOG_FILE${NC}"
