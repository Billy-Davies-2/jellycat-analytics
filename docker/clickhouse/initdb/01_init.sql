CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.jellycat_sentiments (
    id String,
    sentiment String
) ENGINE = MergeTree()
ORDER BY id;
