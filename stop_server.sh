#!/bin/bash

# Mabinogi Mobile 랭킹 서버 중지 스크립트

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 설정
PROJECT_DIR="/root/mobi_rank"
PID_FILE="$PROJECT_DIR/server.pid"

# PID 파일 확인
if [ ! -f "$PID_FILE" ]; then
    echo -e "${YELLOW}서버가 실행 중이지 않습니다 (PID 파일 없음)${NC}"
    exit 1
fi

PID=$(cat "$PID_FILE")

# 프로세스 확인
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${YELLOW}PID $PID 프로세스가 실행 중이지 않습니다${NC}"
    rm -f "$PID_FILE"
    exit 1
fi

# 서버 중지
echo -e "${GREEN}서버 중지 중... (PID: $PID)${NC}"
kill "$PID"

# Graceful shutdown 대기 (최대 10초)
for i in {1..10}; do
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ 서버가 정상적으로 종료되었습니다${NC}"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
    echo -n "."
done

# 강제 종료
echo ""
echo -e "${YELLOW}강제 종료 중...${NC}"
kill -9 "$PID" 2>/dev/null

# Chrome/ChromeDriver 프로세스도 정리
killall -9 chrome chromedriver 2>/dev/null || true

sleep 1

if ! ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ 서버가 강제 종료되었습니다${NC}"
    rm -f "$PID_FILE"
    exit 0
else
    echo -e "${RED}❌ 서버 종료 실패${NC}"
    exit 1
fi
