CREATE TABLE mabinogi_population_statistics (
    id            serial PRIMARY KEY,
    date          date NOT NULL, -- 통계 날짜(YYYY-MM-DD)
    server_name   varchar(50) NOT NULL, -- 서버명, 전체는 'all'
    class_name    varchar(50) NOT NULL, -- 직업명, 전체는 'all'
    population    integer NOT NULL, -- 인구수
    retrieved_at  timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul'),
    div           integer DEFAULT 1 NOT NULL,
    CONSTRAINT unique_population_statistics UNIQUE (date, server_name, class_name, div)
);

CREATE INDEX idx_population_statistics_date ON mabinogi_population_statistics (date);
CREATE INDEX idx_population_statistics_server ON mabinogi_population_statistics (server_name);
CREATE INDEX idx_population_statistics_class ON mabinogi_population_statistics (class_name);