-- 서버별 전체 인구수 테이블
CREATE TABLE IF NOT EXISTS server_population_stats (
    id SERIAL PRIMARY KEY,
    server_name VARCHAR(20) NOT NULL,
    population INTEGER NOT NULL,
    rank_range_min INTEGER,
    rank_range_max INTEGER,
    retrieved_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 직업별 인구수 통계 테이블
CREATE TABLE IF NOT EXISTS class_population_stats (
    id SERIAL PRIMARY KEY,
    server_name VARCHAR(20) NOT NULL,
    class_name VARCHAR(50) NOT NULL,
    character_count INTEGER NOT NULL,
    percentage DECIMAL(5,2),
    average_power BIGINT,
    retrieved_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 일자별 서버 통계 요약 테이블 (일별 집계용)
CREATE TABLE IF NOT EXISTS daily_server_stats (
    id SERIAL PRIMARY KEY,
    server_name VARCHAR(20) NOT NULL,
    date_kst DATE NOT NULL,
    avg_population INTEGER NOT NULL,
    max_population INTEGER,
    min_population INTEGER,
    population_change INTEGER,
    most_popular_class VARCHAR(50),
    top_power_class VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(server_name, date_kst)
);

-- 전투력 분포 테이블 (전투력 구간별 캐릭터 수)
CREATE TABLE IF NOT EXISTS power_distribution_stats (
    id SERIAL PRIMARY KEY,
    server_name VARCHAR(20) NOT NULL,
    power_range_start BIGINT NOT NULL,
    power_range_end BIGINT NOT NULL,
    character_count INTEGER NOT NULL,
    percentage DECIMAL(5,2),
    retrieved_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_server_population_server_date ON server_population_stats(server_name, retrieved_at);
CREATE INDEX IF NOT EXISTS idx_class_population_server_date ON class_population_stats(server_name, retrieved_at);
CREATE INDEX IF NOT EXISTS idx_class_population_class_date ON class_population_stats(class_name, retrieved_at);
CREATE INDEX IF NOT EXISTS idx_daily_server_stats_date ON daily_server_stats(date_kst);
CREATE INDEX IF NOT EXISTS idx_power_distribution_server_date ON power_distribution_stats(server_name, retrieved_at);
