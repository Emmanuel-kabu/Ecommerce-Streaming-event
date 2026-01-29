# 🚀 E-commerce Streaming Analytics Platform - User Guide

## Prerequisites

Before running the project, ensure you have the following installed:

### Required Software
- **Docker Desktop** (version 20.10+)
- **Docker Compose** (version 2.0+)
- **Python** (version 3.8+)
- **Git** (for cloning the repository)

### System Requirements
- **RAM:** 8GB minimum (16GB recommended)
- **Disk Space:** 10GB free space
- **OS:** Windows 10+, macOS 10.15+, or Linux Ubuntu 18.04+

## Quick Start Guide

### Step 1: Clone and Setup
```bash
# Clone the repository
git clone <repository-url>
cd ecommerce_streaming

# Create Python virtual environment
python -m venv stream_env
stream_env\Scripts\activate  # Windows
# or
source stream_env/bin/activate  # Linux/macOS

# Install Python dependencies
pip install -r requirements.txt
```

### Step 2: Environment Configuration
```bash
# Copy environment template
copy .env.example .env  # Windows
# or
cp .env.example .env  # Linux/macOS

# Edit .env file with your settings
notepad .env  # Windows
# or
nano .env  # Linux/macOS
```

**Required Environment Variables:**
```env
# Database Settings
POSTGRES_DB=ecommerce_analytics
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password

# pgAdmin Settings
PGADMIN_DEFAULT_EMAIL=admin@ecommerce.com
PGADMIN_DEFAULT_PASSWORD=admin_password

# Data Generation Settings
DATA_GENERATION_INTERVAL=5
BATCH_SIZE=100
LOG_LEVEL=INFO
```

### Step 3: Start Infrastructure Services
```bash
# Navigate to docker directory
cd docker

# Start PostgreSQL and pgAdmin
docker-compose up -d postgresql pgadmin

# Wait for services to be ready (30-60 seconds)
docker-compose ps
```

### Step 4: Initialize Database
```bash
# Return to project root
cd ..

# Run database setup script
docker exec -i ecommerce_postgresql psql -U postgres -d ecommerce_analytics < postgres_setup.sql

# Verify database setup
docker exec ecommerce_postgresql psql -U postgres -d ecommerce_analytics -c "\dt ecommerce.*"
```

### Step 5: Start Data Generation
```bash
# Open new terminal window
cd ecommerce_streaming
stream_env\Scripts\activate  # Windows
# or
source stream_env/bin/activate  # Linux/macOS

# Start data generator
cd data_generator
python data_generator.py

# You should see log messages indicating data generation has started
```

### Step 6: Start Spark Streaming (New Terminal)
```bash
# Open another terminal window
cd ecommerce_streaming
stream_env\Scripts\activate  # Windows
# or
source stream_env/bin/activate  # Linux/macOS

# Start Spark streaming processor
cd spark_streaming_to_postgres
python spark_to_postgres.py

# You should see Spark initialization and streaming messages
```

## Verification Steps

### 1. Check Services Status
```bash
# Check Docker containers
docker ps

# Expected containers:
# - ecommerce_postgresql
# - ecommerce_pgadmin
```

### 2. Verify Data Generation
```bash
# Check generated CSV files
ls -la data/incoming/

# Should see timestamped CSV files being created every few seconds
```

### 3. Verify Database Ingestion
```bash
# Connect to database and check record count
docker exec ecommerce_postgresql psql -U postgres -d ecommerce_analytics -c "SELECT COUNT(*) FROM ecommerce.ecommerce_events;"

# Should show increasing count as data is processed
```

### 4. Access pgAdmin Web Interface
1. Open browser to: http://localhost:8080
2. Login with credentials from .env file
3. Connect to PostgreSQL server:
   - Host: postgresql (container name)
   - Port: 5432
   - Username: postgres
   - Password: (from .env file)

## Advanced Configuration

### Customizing Data Generation
Edit `data_generator/data_generator.py` configuration:

```python
# Modify these parameters in the script
BATCH_SIZE = 100  # Events per batch
GENERATION_INTERVAL = 5  # Seconds between batches
CATEGORIES = ['Electronics', 'Clothing', 'Sports', 'Books', 'Home & Garden']
```

### Tuning Spark Streaming
Edit `spark_streaming_to_postgres/spark_to_postgres.py`:

```python
# Modify Spark configuration
MICRO_BATCH_DURATION = "5 seconds"  # Processing interval
MAX_FILES_PER_TRIGGER = 10  # Files per micro-batch
```

### Scaling Spark Cluster
To add Spark worker nodes, edit `docker/docker-compose.yml`:

```yaml
# Add worker nodes
spark-worker-2:
  image: apache/spark-py:latest
  container_name: ecommerce_spark_worker_2
  # ... configuration
```

## Monitoring and Troubleshooting

### Log Files Locations
```
logs/
├── data_generator_YYYYMMDD.log
├── spark_streaming_YYYYMMDD.log
└── system_metrics.log
```

### Common Issues and Solutions

#### 1. Container Startup Failures
```bash
# Check container logs
docker logs ecommerce_postgresql
docker logs ecommerce_pgadmin

# Common fix: restart services
docker-compose restart
```

#### 2. Database Connection Issues
```bash
# Test database connectivity
docker exec ecommerce_postgresql pg_isready -U postgres

# Reset database if needed
docker-compose down -v
docker-compose up -d postgresql
```

#### 3. Spark Streaming Errors
```bash
# Check Spark logs
cat logs/spark_streaming_*.log

# Common issues:
# - Insufficient memory: increase Docker memory limit
# - Port conflicts: check port availability
# - Missing dependencies: reinstall requirements
```

#### 4. Data Generation Not Working
```bash
# Check Python environment
python --version
pip list

# Verify CSV file creation
ls -la data/incoming/

# Check for permission issues
chmod 755 data/incoming/
```

### Performance Monitoring
```bash
# Monitor system resources
docker stats

# Check database performance
docker exec ecommerce_postgresql psql -U postgres -d ecommerce_analytics -c "SELECT * FROM ecommerce.performance_metrics ORDER BY metric_timestamp DESC LIMIT 10;"

# View data quality metrics
docker exec ecommerce_postgresql psql -U postgres -d ecommerce_analytics -c "SELECT * FROM ecommerce.data_quality_metrics ORDER BY batch_timestamp DESC LIMIT 5;"
```

## Stopping the System

### Graceful Shutdown
```bash
# Stop data generator (Ctrl+C in its terminal)

# Stop Spark streaming (Ctrl+C in its terminal)

# Stop Docker services
cd docker
docker-compose down

# Preserve data volumes
docker-compose down --volumes  # Only if you want to reset data
```

### Clean Restart
```bash
# Complete reset (removes all data)
docker-compose down -v
docker system prune -f

# Start fresh
docker-compose up -d
```

## Customization Options

### Adding New Event Types
1. Modify `data_generator.py` to include new event types
2. Update database schema in `postgres_setup.sql`
3. Adjust Spark processing logic if needed

### Integrating External Data Sources
1. Create new input connectors in Spark
2. Update schema validation rules
3. Modify database table structure as needed

### Adding Real-time Dashboards
1. Connect BI tools to PostgreSQL
2. Use materialized views for fast queries
3. Set up automated refresh schedules

## Next Steps

1. **Explore Analytics:** Use pgAdmin to query the data
2. **Add Dashboards:** Connect Grafana or similar tools
3. **Scale Up:** Add more Spark workers for higher throughput
4. **Monitor:** Set up alerting for system health
5. **Optimize:** Tune parameters based on your data volume

For additional help, check the logs directory or consult the project documentation.