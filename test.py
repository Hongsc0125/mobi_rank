# mass_rank_fetcher.py

import json
import time
import logging
from every_rank_data_task import get_driver, fetch_rank_page, parse_rank_html, switch_server
from service.db import insert_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mass_rank_fetcher")

def load_character_list(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def mass_rank_crawl(json_list, div=1):
    driver = get_driver()
    results = []

    for entry in json_list:
        server_name = entry['server_name']
        character_name = entry['character_name']

        server_num = switch_server(server_name)

        try:
            html = fetch_rank_page(driver, server_num, character_name, div)
            data = parse_rank_html(html)

            # 해당 캐릭터와 정확히 일치하는 데이터만 필터링
            if data:
                results.extend(data)
                logger.info(f"[{server_name}] {character_name}: {len(data)}개 데이터 수집됨")
            else:
                logger.warning(f"[{server_name}] {character_name}: 결과 없음")

        except Exception as e:
            logger.error(f"[{server_name}] {character_name} 처리 중 오류: {e}")

    driver.quit()

    # 데이터 저장
    if results:
        insert_data(results, server=None, character=None, div=div)
        logger.info(f"총 {len(results)}개 항목 저장 완료")

    return results

if __name__ == "__main__":
    json_file = "test.json"
    char_list = load_character_list(json_file)
    result = mass_rank_crawl(char_list)
    logger.info("모든 캐릭터 데이터 수집 완료")
    print(result)
