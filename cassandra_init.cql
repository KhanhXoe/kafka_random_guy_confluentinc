-- ============================================
-- Cassandra Init Script
-- Automatically run when container starts
-- ============================================

-- Create keyspace for spark streaming data
CREATE KEYSPACE IF NOT EXISTS spark_streaming
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create table to store user data from Kafka stream
CREATE TABLE IF NOT EXISTS spark_streaming.user_created (
    user_id text PRIMARY KEY,
    name text,
    age int,
    gender text,
    email text,
    phone text,
    cell text,
    id text,
    picture text,
    nat text,
    registered_date text
);
