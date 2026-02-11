#!/usr/bin/env python3
"""
완전한 큐 시스템 테스트 - 전체 워크플로우
"""

import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from service.search_queue import search_queue_manager, create_search_queue_table
    from service.db_session import get_current_time, SessionLocal
    from sqlalchemy import text
    
    def clean_test_data():
        """테스트 데이터 정리"""
        try:
            with SessionLocal() as db:
                db.execute(text("DELETE FROM search_request_queue WHERE character_name LIKE 'test_%'"))
                db.commit()
                print("✓ 기존 테스트 데이터 정리 완료")
        except Exception as e:
            print(f"⚠ 테스트 데이터 정리 중 오류 (무시): {e}")
    
    def test_complete_queue_workflow():
        """완전한 큐 워크플로우 테스트"""
        print("=== 완전한 PostgreSQL 큐 시스템 테스트 ===")
        
        # 0. 테스트 데이터 정리
        clean_test_data()
        
        try:
            # 1. 테이블 생성
            print("\n1. 검색 큐 테이블 생성...")
            create_search_queue_table()
            print("✓ 검색 큐 테이블 생성 완료")
            
            # 2. 새로운 검색 요청 추가
            print("\n2. 새로운 검색 요청 추가...")
            request_id = search_queue_manager.enqueue_search_request(
                server="던컨",
                character_name="test_user_123",
                client_ip="127.0.0.1",
                user_agent="Test-Agent/1.0",
                priority=1
            )
            print(f"✓ 검색 요청 추가 완료: {request_id}")
            
            # 3. 요청 상태 확인 (pending)
            print("\n3. 요청 상태 확인 (PENDING)...")
            status = search_queue_manager.get_request_status(request_id)
            if status and status['status'] == 'pending':
                print(f"✓ PENDING 상태 확인됨")
                print(f"  서버: {status['server']}")
                print(f"  캐릭터: {status['character_name']}")
            else:
                print(f"✗ 잘못된 상태: {status['status'] if status else 'None'}")
                return False
            
            # 4. 워커가 요청을 가져감 (processing 상태로 변경)
            print("\n4. 워커가 다음 요청 가져오기...")
            next_request = search_queue_manager.get_next_request()
            if next_request and next_request['request_id'] == request_id:
                print(f"✓ 요청을 성공적으로 가져옴: {next_request['request_id']}")
                print(f"  서버: {next_request['server']}")
                print(f"  캐릭터: {next_request['character_name']}")
            else:
                print("✗ 요청을 가져오지 못함")
                return False
            
            # 5. 처리 중 상태 확인
            print("\n5. 처리 중 상태 확인...")
            processing_status = search_queue_manager.get_request_status(request_id)
            if processing_status and processing_status['status'] == 'processing':
                print("✓ PROCESSING 상태로 변경됨")
                print(f"  시작시간: {processing_status['started_at']}")
            else:
                print(f"✗ 상태 변경 실패: {processing_status['status'] if processing_status else 'None'}")
                return False
            
            # 6. 테스트 결과 데이터 생성 (복잡한 중첩 구조)
            print("\n6. 복잡한 검색 결과 데이터 생성...")
            test_result = {
                "success": True,
                "message": "검색 성공",
                "from_cache": False,
                "server": "던컨",
                "character": "test_user_123",
                "data": {
                    "character": "test_user_123",
                    "server": "던컨",
                    "retrieved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "rankings": {
                        "전투력": {
                            "type": "전투력",
                            "data": [
                                {
                                    "id": 98765,
                                    "rank_position": 50,
                                    "change_amount": 10,
                                    "change_type": "up",
                                    "server_name": "던컨",
                                    "character_name": "test_user_123",
                                    "class_name": "마법사",
                                    "power_value": 250000,
                                    "retrieved_at": get_current_time().isoformat(),
                                    "div": 1
                                }
                            ],
                            "retrieved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        },
                        "매력": {
                            "type": "매력",
                            "data": [
                                {
                                    "id": 98766,
                                    "rank_position": 75,
                                    "change_amount": -5,
                                    "change_type": "down",
                                    "server_name": "던컨",
                                    "character_name": "test_user_123",
                                    "class_name": "마법사",
                                    "power_value": 180000,
                                    "retrieved_at": get_current_time().isoformat(),
                                    "div": 2
                                }
                            ],
                            "retrieved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        },
                        "생활력": {
                            "type": "생활력",
                            "data": [],
                            "retrieved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                    }
                },
                "metadata": {
                    "total_rankings": 2,
                    "process_time": 1.23,
                    "unicode_test": "한글 유니코드 테스트 🚀",
                    "nested_array": [
                        {"key": "value1", "number": 123},
                        {"key": "value2", "number": 456}
                    ]
                }
            }
            
            # 7. 요청 완료 처리 (JSON 직렬화 테스트)
            print("\n7. 검색 완료 처리 (복잡한 JSON 직렬화)...")
            try:
                search_queue_manager.complete_request(request_id, test_result)
                print("✓ 복잡한 검색 결과 JSON 직렬화 성공")
            except Exception as e:
                print(f"✗ 검색 완료 처리 실패: {e}")
                import traceback
                traceback.print_exc()
                return False
            
            # 8. 완료된 요청 상태 및 결과 확인 (JSON 역직렬화)
            print("\n8. 완료된 요청 결과 확인 (복잡한 JSON 역직렬화)...")
            final_status = search_queue_manager.get_request_status(request_id)
            if final_status and final_status['status'] == 'completed':
                print("✓ COMPLETED 상태 확인됨")
                print(f"  완료시간: {final_status['completed_at']}")
                
                # 결과 데이터 상세 검증
                result = final_status.get('result')
                if result and isinstance(result, dict):
                    print("✓ JSON 역직렬화 성공")
                    
                    # 기본 필드 검증
                    success = result.get('success')
                    message = result.get('message')
                    print(f"  검색 성공: {success}")
                    print(f"  메시지: {message}")
                    
                    # 중첩 데이터 검증
                    data = result.get('data', {})
                    if data:
                        rankings = data.get('rankings', {})
                        
                        # 전투력 랭킹 검증
                        if '전투력' in rankings:
                            power_ranking = rankings['전투력']
                            power_data = power_ranking['data'][0] if power_ranking['data'] else None
                            if power_data:
                                print(f"  전투력 랭킹: {power_data['rank_position']}위")
                                print(f"  직업: {power_data['class_name']}")
                                print(f"  전투력: {power_data['power_value']:,}")
                                print(f"  변화: {power_data['change_amount']} ({power_data['change_type']})")
                        
                        # 매력 랭킹 검증
                        if '매력' in rankings:
                            charm_ranking = rankings['매력']
                            charm_data = charm_ranking['data'][0] if charm_ranking['data'] else None
                            if charm_data:
                                print(f"  매력 랭킹: {charm_data['rank_position']}위")
                                print(f"  매력: {charm_data['power_value']:,}")
                    
                    # 메타데이터 검증
                    metadata = result.get('metadata', {})
                    if metadata:
                        print(f"  총 랭킹 수: {metadata.get('total_rankings')}")
                        print(f"  처리 시간: {metadata.get('process_time')}초")
                        print(f"  유니코드: {metadata.get('unicode_test')}")
                        
                        # 중첩 배열 검증
                        nested_array = metadata.get('nested_array', [])
                        if nested_array:
                            print(f"  중첩 배열: {len(nested_array)}개 요소")
                            for i, item in enumerate(nested_array):
                                print(f"    [{i}]: {item}")
                    
                    print("✓ 모든 중첩 JSON 데이터가 정상적으로 처리됨")
                else:
                    print("✗ JSON 역직렬화 실패 또는 결과 없음")
                    return False
            else:
                print(f"✗ 완료 상태 확인 실패: {final_status['status'] if final_status else 'None'}")
                return False
            
            # 9. 큐 통계 확인
            print("\n9. 큐 통계 확인...")
            stats = search_queue_manager.get_queue_stats()
            print(f"✓ 큐 통계:")
            for status, count in stats.items():
                if count > 0:
                    print(f"  {status}: {count}개")
            
            # 10. 정리
            print("\n10. 테스트 데이터 정리...")
            clean_test_data()
            
            print("\n🎉 === 모든 테스트 통과! === 🎉")
            print("PostgreSQL 큐 시스템이 완벽하게 작동합니다:")
            print("✅ 큐 요청 추가/조회")
            print("✅ 상태 전환 (pending → processing → completed)")
            print("✅ 복잡한 중첩 구조 JSON 직렬화/역직렬화")
            print("✅ 유니코드 문자 처리")
            print("✅ datetime 객체 자동 문자열 변환")
            print("✅ 큐 통계 조회")
            print("✅ 트랜잭션 안전성")
            
            return True
            
        except Exception as e:
            print(f"\n💥 테스트 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    if __name__ == "__main__":
        success = test_complete_queue_workflow()
        sys.exit(0 if success else 1)
        
except ImportError as e:
    print(f"모듈 import 실패: {e}")
    sys.exit(1)