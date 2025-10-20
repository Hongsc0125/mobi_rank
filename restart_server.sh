#!/bin/bash

# Mabinogi Mobile 랭킹 서버 재시작 스크립트

# 색상 정의
GREEN='\033[0;32m'
NC='\033[0m' # No Color

PROJECT_DIR="/root/mobi_rank"

cd "$PROJECT_DIR" || exit 1

echo -e "${GREEN}서버 재시작 중...${NC}"
echo ""

# 서버 중지
./stop_server.sh

# 잠시 대기
sleep 2

# 서버 시작
./start_server.sh
