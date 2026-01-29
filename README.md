# 🛒 E-commerce Real-Time Streaming Analytics Platform

A comprehensive real-time data streaming platform for e-commerce analytics using **Apache Spark**, **PostgreSQL**, and **Docker**. This system generates realistic e-commerce events, processes them in real-time using Spark Structured Streaming, and stores them in PostgreSQL for analytics.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Data Generator │───▶│  CSV Files      │───▶│ Spark Streaming │
│  (Python/Faker)│    │  (data/incoming)│    │   Processing    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │◀───│   pgAdmin       │    │   Spark Cluster │
│   Database      │    │ (Web Interface) │    │  Master/Worker  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Features

### **Real-Time Data Processing**
- **Spark Structured Streaming** for real-time event processing
- **Micro-batch processing** with configurable intervals
- **Data validation** and quality checks at multiple layers
- **Automatic schema enforcement** and evolution

### **Comprehensive Data Generation**
- **Realistic e-commerce events**: views, purchases, cart actions, clicks
- **Multi-category products**: Electronics, Clothing, Books, Sports, Home & Garden
- **Rich customer data**: demographics, session tracking, device information
- **Configurable generation rates** and batch sizes

### **Advanced Monitoring & Logging**
- **Professional logging** with file rotation and multiple log levels
- **Real-time statistics** tracking (throughput, success rates, error rates)
- **Data quality metrics** (null values, duplicates, validation failures)
- **Performance monitoring** (processing times, batch sizes, resource usage)

### **Enterprise-Grade Reliability**
- **Comprehensive error handling** with retry logic
- **Graceful shutdown** and resource cleanup
- **Data validation** at ingestion and processing stages
- **Health checks** for all services

### **Containerized Deployment**
- **Docker Compose** for easy deployment
- **Service discovery** and networking
- **Persistent volumes** for data durability
- **Health checks** and auto-restart policies

## 📁 Project Structure

```
ecommerce_streaming/
├── data_generator/             # CSV event generator (writes into data/incoming)
│   └── data_generator.py
├── spark_streaming_to_postgres/ # Spark Structured Streaming job (CSV -> Postgres)
│   └── spark_to_postgres.py
├── docker/                     # Docker Compose + job container
│   ├── docker-compose.yml
│   ├── Dockerfile.python
│   └── .env                    # Compose environment (db creds, JDBC URL, etc.)
├── sql_setup/                  # Optional schema / analytics setup
│   └── postgres_setup.sql
├── tests/                      # Test + data generation helpers
│   ├── generate_test_data.py
│   └── test_stream_to_postgres.py
├── data/                       # Mounted into containers as /app/data
│   ├── incoming/               # Drop CSVs here for the stream
│   └── checkpoints/            # Streaming offsets / checkpoints
├── logs/                       # Logs (mounted into spark-job as /app/logs)
├── requirements.txt
├── .dockerignore
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- **Docker & Docker Compose** installed
- **Python 3.11+** (for local development)
- **8GB+ RAM** recommended for Spark cluster

> **Windows note:** Running Spark (PySpark) directly on Windows often requires Hadoop `winutils.exe` and setting `HADOOP_HOME`/`hadoop.home.dir`. If you hit errors like “HADOOP_HOME and hadoop.home.dir are unset”, use the Docker Compose workflow below (recommended).

### 1. Clone and Setup

```bash
git clone <your-repository>
cd ecommerce_streaming

# Review and customize Docker Compose environment
# (this repo keeps it in docker/.env)
notepad docker/.env
```

### 2. Start the Platform

```bash
# Start all services with Docker Compose
cd docker
docker compose up -d --build

# Monitor service startup
docker compose logs -f
```

### 3. Verify Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **pgAdmin** | http://localhost:8080 | admin@admin.com / admin123 |
| **Spark Master UI** | http://localhost:8081 | - |
| **PostgreSQL** | localhost:5432 | postgres / (see docker/.env) |

Note: inside Docker networking, services reach Postgres by host `postgresql`.

### 4. Generate Input Data (Docker-only)

Generate CSVs directly into `data/incoming` using a one-shot container:

```bash
cd docker
docker compose run --rm spark-job python tests/generate_test_data.py --count 500 --batch-size 100 --out-dir data/incoming
```

Or (optional) run the continuous generator locally if you want constant file creation:

```bash
python -m venv stream_env
stream_env\Scripts\activate
pip install -r requirements.txt
cd data_generator
python data_generator.py
```

### 5. Watch the Streaming Job

```bash
cd docker
docker compose logs -f spark-job
```

### 5b. Start Spark Streaming (Docker - recommended on Windows)

The Docker Compose stack includes a `spark-job` container that runs the streaming job inside Linux:

```bash
cd docker
docker compose up -d --build spark-job
docker compose logs -f spark-job
```

## ⚙️ Configuration

### Environment Variables

Configure the platform using environment variables in `.env`:

```bash
# Database Configuration
POSTGRES_DB=ecommerce_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password123
# Spark uses a JDBC URL for Postgres
DATABASE_URL=jdbc:postgresql://postgresql:5432/ecommerce_db

# Spark Configuration
SPARK_MASTER_URL=local[*]
INPUT_DATA_DIR=/app/data/incoming
CHECKPOINT_DIR=/app/data/checkpoints
LOG_LEVEL=INFO                 # DEBUG, INFO, WARNING, ERROR

# Monitoring
ENABLE_METRICS=true
METRICS_PORT=9090
```

### Data Generator Configuration

Customize data generation in `data_generator/data_generator.py`:

```python
# Product categories and realistic names
PRODUCT_CATEGORIES = {
    "Electronics": ["Smart TV", "Laptop", "Headphones", ...],
    "Clothing": ["T-Shirt", "Jeans", "Sneakers", ...],
    # Add custom categories
}

# Event types
EVENT_TYPES = ["view", "purchase", "cart", "click", "add_to_wishlist"]

# Price ranges by category
PRICE_RANGES = {
    "Electronics": (50.0, 2000.0),
    "Clothing": (10.0, 200.0),
    # Customize price ranges
}
```

## Data Schema

### Generated Events Schema

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `event_id` | VARCHAR(100) | Unique event identifier | `550e8400-e29b-41d4-a716-446655440000` |
| `event_type` | VARCHAR(50) | Type of user action | `purchase`, `view`, `cart` |
| `product_id` | INTEGER | Product identifier | `1`, `42`, `157` |
| `product_name` | TEXT | Product name with variations | `Sony Wireless Headphones - Blue Edition` |
| `category` | VARCHAR(100) | Product category | `Electronics`, `Clothing` |
| `brand` | VARCHAR(100) | Product brand | `Sony`, `Nike`, `Apple` |
| `sku` | VARCHAR(50) | Stock keeping unit | `ELE-0001`, `CLO-0025` |
| `price` | DECIMAL(10,2) | Product price | `99.99`, `299.50` |
| `customer_id` | VARCHAR(100) | Customer identifier | `customer-uuid` |
| `customer_email` | VARCHAR(255) | Customer email | `john.doe@example.com` |
| `customer_name` | VARCHAR(255) | Customer name | `John Doe` |
| `customer_address` | TEXT | Customer address | `123 Main St, City, State` |
| `session_id` | VARCHAR(100) | Session identifier | `session-uuid` |
| `user_agent` | TEXT | Browser/device info | `Mozilla/5.0 (Windows...)` |
| `ip_address` | VARCHAR(45) | Customer IP | `192.168.1.100` |
| `price_category` | VARCHAR(20) | Derived price tier | `Low`, `Medium`, `High` |
| `device_type` | VARCHAR(20) | Derived device type | `Mobile`, `Desktop`, `Tablet` |
| `event_timestamp` | TIMESTAMP | Event occurrence time | `2026-01-14 15:30:45.123` |
| `created_at` | TIMESTAMP | Record insertion time | `2026-01-14 15:30:45.456` |

## Monitoring & Analytics

### Real-Time Monitoring

#### Application Logs
- **Data Generator**: `logs/data_generator_YYYYMMDD.log`
- **Spark Streaming**: `logs/spark_streaming_YYYYMMDD.log`

In the Docker workflow, Spark logs are written inside the container to `/app/logs` and are mounted back to the host `logs/` folder.

#### Key Metrics Tracked
- **Throughput**: Events/second, batches/minute
- **Quality**: Success rates, validation failures, error rates
- **Performance**: Processing times, resource utilization
- **Data Quality**: Null values, duplicates, schema violations

### Sample Analytics Queries

```sql
-- Event type distribution over time
SELECT 
    event_type,
    DATE(event_timestamp) as event_date,
    COUNT(*) as event_count
FROM ecommerce_events 
WHERE event_timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY event_type, DATE(event_timestamp)
ORDER BY event_date DESC, event_count DESC;

-- Top products by category
SELECT 
    category,
    product_name,
    COUNT(*) as view_count,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count
FROM ecommerce_events 
WHERE event_timestamp >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY category, product_name
ORDER BY view_count DESC
LIMIT 20;

-- Customer behavior analysis
SELECT 
    device_type,
    AVG(price) as avg_price,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_events
FROM ecommerce_events 
WHERE event_type IN ('view', 'purchase')
GROUP BY device_type;

-- Real-time performance monitoring
SELECT 
    DATE_TRUNC('hour', event_timestamp) as hour,
    COUNT(*) as events_per_hour,
    AVG(price) as avg_price,
    COUNT(DISTINCT customer_id) as unique_customers
FROM ecommerce_events 
WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', event_timestamp)
ORDER BY hour DESC;
```

## 🔧 Development

### Local Development Setup

1. **Python Environment**
```bash
python -m venv stream_env
source stream_env/bin/activate
pip install -r requirements.txt
```

2. **Database Setup**
```bash
# Start only PostgreSQL for development
cd docker
docker compose up -d postgresql pgadmin
```

3. **Run Components Locally**
```bash
# Terminal 1: Data Generator
cd data_generator
python data_generator.py

# Terminal 2: Spark Streaming
cd spark_streaming_to_postgres  
python spark_to_postgres.py
```

### Adding New Features

#### Custom Event Types
1. Add new event type to `EVENT_TYPES` in `data_generator.py`
2. Update event generation logic if needed
3. Modify analytics queries to include new events

#### New Product Categories
1. Add category to `PRODUCT_CATEGORIES` in `data_generator.py`
2. Set appropriate price range in `PRICE_RANGES`
3. Update any category-specific analytics

#### Enhanced Data Validation
1. Add validation rules in `_apply_data_validation()` method
2. Update `_validate_batch_quality()` for new checks
3. Add corresponding monitoring metrics

##  Troubleshooting

### Common Issues

#### 1. **Port Conflicts**
```bash
# On Windows, check whether ports are bound
netstat -ano | findstr ":5432"  # PostgreSQL
netstat -ano | findstr ":8080"  # pgAdmin
netstat -ano | findstr ":7077"  # Spark Master
```

#### 2. **Memory Issues**
```bash
# Reduce Spark worker memory
SPARK_WORKER_MEMORY=1G

# Reduce batch sizes
EVENTS_PER_BATCH=50
```

#### 3. **Database Connection Issues**
```bash
# Check PostgreSQL logs
cd docker
docker compose logs postgresql

# Verify connection
docker exec -it ecommerce_postgresql psql -U postgres -d ecommerce_db
```

#### 4. **Data Generation Stopped**
```bash
# Check logs for errors
tail -f logs/data_generator_*.log

# Check disk space
df -h

# Check permissions
ls -la data/incoming/
```

### Log Analysis

#### High Error Rates
```bash
# Search for errors in logs
grep -i "error" logs/*.log
grep -i "failed" logs/*.log

# Check data validation failures
grep "validation rejected" logs/*.log
```

#### Performance Issues
```bash
# Check processing times
grep "batch.*completed" logs/spark_streaming_*.log

# Monitor resource usage
docker stats
```

## Security Considerations

### Production Deployment
- **Change default passwords** in `.env`
- **Use environment-specific configurations**
- **Enable SSL/TLS** for database connections
- **Implement proper authentication** for web interfaces
- **Use secrets management** for sensitive data
- **Configure firewalls** and network security
- **Regular security updates** for all components

### Data Privacy
- **Implement data retention policies**
- **Add data anonymization** for sensitive fields
- **GDPR compliance** for customer data
- **Audit logging** for data access

##  Performance Tuning

### Spark Optimization
```bash
# Increase parallelism
spark.sql.shuffle.partitions=200

# Memory tuning
spark.executor.memory=2g
spark.driver.memory=1g

# Optimize for streaming
spark.sql.streaming.checkpointLocation.fallback=/app/data/checkpoints
```

### PostgreSQL Tuning
```sql
-- Optimize for write-heavy workload
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET checkpoint_segments = '32';
```

## Contributing

1. **Fork the repository**
2. **Create a feature branch** (`git checkout -b feature/amazing-feature`)
3. **Make your changes** with comprehensive logging and tests
4. **Update documentation** including this README
5. **Commit your changes** (`git commit -m 'Add amazing feature'`)
6. **Push to the branch** (`git push origin feature/amazing-feature`)
7. **Open a Pull Request**

### Code Standards
- **Comprehensive logging** for all new features
- **Data validation** for all inputs
- **Error handling** with proper exception types
- **Type hints** for all function parameters
- **Docstrings** for all classes and methods

##  License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏Acknowledgments

- **Apache Spark** for real-time stream processing
- **PostgreSQL** for reliable data storage
- **Docker** for containerization
- **Faker** for realistic data generation
- **Bitnami** for Spark Docker images

---

## Support

For questions, issues, or contributions:

- **Issues**: Open a GitHub issue with detailed description
- **Documentation**: Check this README and inline code documentation
- **Logs**: Always include relevant log files with issue reports
- **Configuration**: Provide your environment configuration (without sensitive data)

**Happy Streaming! **
