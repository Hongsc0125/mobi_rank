#!/usr/bin/env python3
"""
Simple Sequential Crawler - Chrome 안정성을 위한 단순 순차 크롤러
server.py와 동일한 방식으로 service/full_data.py 사용
"""

import time
import logging
from service.full_data import fetch_rank_via_dom, parse_rank_html
from service.db import insert_data

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def simple_crawl_server(server_name, search_characters=None):
    """단일 서버 크롤링 (server.py와 동일한 방식)"""
    logger.info(f"서버 {server_name} 크롤링 시작")
    
    if not search_characters:
        search_characters = ["가", "나", "다", "라", "마", "바", "사", "아", "자", "차", "카", "타", "파", "하"]
    
    all_data = []
    
    for idx, character in enumerate(search_characters):
        try:
            logger.info(f"서버 {server_name} [{idx+1}/{len(search_characters)}] '{character}' 검색 중...")
            
            # service/full_data.py의 검증된 함수 사용 (server.py와 동일)
            html_data = fetch_rank_via_dom(server=server_name, name=character, rank_type=1)
            
            if html_data:
                # HTML 파싱
                parsed_data = parse_rank_html(html_data)
                if parsed_data:
                    # DB 저장
                    result = insert_data(parsed_data, server=server_name, div=1)
                    logger.info(f"서버 {server_name} '{character}' 검색: {len(parsed_data)}개 저장, 성공: {result.get('success', False)}")
                    all_data.extend(parsed_data)
                else:
                    logger.warning(f"서버 {server_name} '{character}' 파싱 결과 없음")
            else:
                logger.warning(f"서버 {server_name} '{character}' 검색 결과 없음")
                
            # 안정성을 위한 대기
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"서버 {server_name} '{character}' 처리 오류: {e}")
            time.sleep(5)  # 오류 시 더 긴 대기
            continue
    
    logger.info(f"서버 {server_name} 크롤링 완료: 총 {len(all_data)}개 데이터 수집")
    return all_data

def main():
    """메인 실행 함수"""
    servers = ["데이안", "아이라", "던컨", "알리사", "메이븐", "라사", "칼릭스"]
    
    logger.info("=== Simple Sequential Crawler 시작 ===")
    logger.info("server.py와 동일한 안정적인 방식 사용")
    
    while True:
        try:
            for server in servers:
                try:
                    simple_crawl_server(server)
                    logger.info(f"서버 {server} 완료, 5초 대기 후 다음 서버")
                    time.sleep(5)  # 서버 간 대기
                    
                except Exception as e:
                    logger.error(f"서버 {server} 크롤링 실패: {e}")
                    time.sleep(10)  # 오류 시 더 긴 대기
            
            logger.info("모든 서버 크롤링 완료, 1분 대기 후 다음 사이클")
            time.sleep(60)  # 사이클 간 대기
            
        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단됨")
            break
        except Exception as e:
            logger.error(f"예상치 못한 오류: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()