-- PostgreSQL setup script for E-commerce Streaming Analytics Platform
-- This script creates the database, schema, and tables for storing e-commerce events

-- Create database (uncomment if running as superuser)
-- CREATE DATABASE ecommerce_analytics;

-- Connect to the database
\c ecommerce_analytics;

-- Create schema for e-commerce data
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Set default schema
SET search_path TO ecommerce;

-- Create sequence for auto-incrementing IDs
CREATE SEQUENCE IF NOT EXISTS ecommerce_events_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Create main e-commerce events table
CREATE TABLE IF NOT EXISTS ecommerce_events (
    id BIGINT PRIMARY KEY DEFAULT nextval('ecommerce_events_seq'),
    event_id VARCHAR(36) NOT NULL UNIQUE,
    event_type VARCHAR(50) NOT NULL,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    brand VARCHAR(255) NOT NULL,
    sku VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    customer_id VARCHAR(36) NOT NULL,
    customer_email VARCHAR(255) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_address TEXT NOT NULL,
    session_id VARCHAR(36) NOT NULL,
    user_agent TEXT NOT NULL,
    ip_address INET NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_event_type ON ecommerce_events(event_type);
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_category ON ecommerce_events(category);
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_customer_id ON ecommerce_events(customer_id);
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_session_id ON ecommerce_events(session_id);
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_timestamp ON ecommerce_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_product_id ON ecommerce_events(product_id);
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_brand ON ecommerce_events(brand);

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_customer_timestamp ON ecommerce_events(customer_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_category_timestamp ON ecommerce_events(category, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_type_timestamp ON ecommerce_events(event_type, event_timestamp);

-- Create materialized view for real-time analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce_analytics_summary AS
SELECT 
    DATE_TRUNC('hour', event_timestamp) AS hour_bucket,
    event_type,
    category,
    COUNT(*) as event_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT session_id) as unique_sessions,
    AVG(price) as avg_price,
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as total_revenue,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event
FROM ecommerce_events
GROUP BY 
    DATE_TRUNC('hour', event_timestamp),
    event_type,
    category;

-- Create unique index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_ecommerce_analytics_summary_unique 
ON ecommerce_analytics_summary(hour_bucket, event_type, category);

-- Create function to refresh materialized view
CREATE OR REPLACE FUNCTION refresh_ecommerce_analytics()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY ecommerce_analytics_summary;
END;
$$ LANGUAGE plpgsql;

-- Create trigger function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_ecommerce_events_updated_at
    BEFORE UPDATE ON ecommerce_events
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create table for tracking data quality metrics
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id BIGSERIAL PRIMARY KEY,
    batch_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    total_records INTEGER NOT NULL,
    valid_records INTEGER NOT NULL,
    invalid_records INTEGER NOT NULL,
    duplicate_records INTEGER NOT NULL,
    null_values_count INTEGER NOT NULL,
    processing_time_ms BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index on data quality metrics
CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_timestamp ON data_quality_metrics(batch_timestamp);

-- Create table for performance metrics
CREATE TABLE IF NOT EXISTS performance_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    metric_type VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15, 4) NOT NULL,
    metric_unit VARCHAR(50) NOT NULL,
    additional_info JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index on performance metrics
CREATE INDEX IF NOT EXISTS idx_performance_metrics_timestamp ON performance_metrics(metric_timestamp);
CREATE INDEX IF NOT EXISTS idx_performance_metrics_type ON performance_metrics(metric_type);

-- Grant permissions (adjust as needed for your environment)
-- GRANT USAGE ON SCHEMA ecommerce TO spark_user;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ecommerce TO spark_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ecommerce TO spark_user;

-- Insert some sample data for testing (optional)
INSERT INTO ecommerce_events (
    event_id, event_type, product_id, product_name, category, brand, sku, price,
    customer_id, customer_email, customer_name, customer_address, session_id,
    user_agent, ip_address, event_timestamp
) VALUES (
    'test-event-001', 'view', 1, 'Test Product', 'Electronics', 'Test Brand', 'TEST-001', 99.99,
    'test-customer-001', 'test@example.com', 'Test Customer', '123 Test Street, Test City', 'test-session-001',
    'Mozilla/5.0 Test Browser', '192.168.1.100', CURRENT_TIMESTAMP
) ON CONFLICT (event_id) DO NOTHING;

-- Display table information
\dt ecommerce.*

-- Display successful setup message
SELECT 'PostgreSQL setup completed successfully!' as status;