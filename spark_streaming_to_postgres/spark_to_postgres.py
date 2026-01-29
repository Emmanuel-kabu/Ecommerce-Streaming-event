import os
import sys
import logging
import traceback
import platform
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    col, to_timestamp, when, regexp_extract, lower, trim,
    isnan, isnull, size, split, count as spark_count,
    min as spark_min, max as spark_max, avg as spark_avg
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

def _load_env_files() -> None:
    """Load environment variables from common project .env locations.

    This project commonly stores Docker Compose variables in docker/.env.
    For local runs, a repo-root .env is also supported.
    """

    try:
        repo_root = Path(__file__).resolve().parents[1]
    except Exception:
        load_dotenv(override=False)
        return

    candidates = [
        repo_root / ".env",
        repo_root / "docker" / ".env",
    ]

    loaded_any = False
    for candidate in candidates:
        if candidate.exists():
            load_dotenv(dotenv_path=candidate, override=False)
            loaded_any = True

    if not loaded_any:
        load_dotenv(override=False)


def _is_windows() -> bool:
    return os.name == "nt" or platform.system().lower().startswith("win")


def _windows_has_winutils() -> bool:
    hadoop_home = os.getenv("HADOOP_HOME") or os.getenv("hadoop.home.dir")
    if not hadoop_home:
        return False

    try:
        return (Path(hadoop_home) / "bin" / "winutils.exe").exists()
    except Exception:
        return False


_load_env_files()

def setup_comprehensive_logging(log_level: str = "INFO", log_dir: str = "logs") -> logging.Logger:
    """Setup comprehensive logging with file and console handlers."""
    
    # Create logs directory
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # Create logger
    logger = logging.getLogger("SparkStreamingProcessor")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # File handler
    log_filename = log_path / f"spark_streaming_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

class EcommerceStreamingJob:
    """
    Spark Structured Streaming job that reads
    CSV events and writes them to PostgreSQL with comprehensive
    logging, validation, and error handling.
    """

    def __init__(
        self,
        spark_master: str,
        input_path: str,
        checkpoint_path: str,
        db_url: str = None,
        db_table: str = "ecommerce_events",
        db_user: str = None,
        db_password: str = None,
        log_level: str = "INFO"
    ):
        # Setup logging first
        self.logger = setup_comprehensive_logging(log_level, "logs")
        self.logger.info("Initializing EcommerceStreamingJob...")
        
        # Statistics tracking
        self.total_batches_processed = 0
        self.total_records_processed = 0
        self.total_records_failed = 0
        self.start_time = datetime.now(timezone.utc)
        
        try:
            # Validate and set configuration
            self._validate_configuration(
                spark_master, input_path, checkpoint_path, 
                db_url, db_table, db_user, db_password
            )
            
            self.spark_master = spark_master
            self.input_path = input_path
            self.checkpoint_path = checkpoint_path
            self.db_url = db_url or os.getenv("DATABASE_URL")
            self.db_table = db_table
            self.db_user = db_user or os.getenv("POSTGRES_USER")
            self.db_password = db_password or os.getenv("POSTGRES_PASSWORD")
            
            self.logger.debug(f"Configuration validated: master={spark_master}, input={input_path}")
            
            # Initialize Spark Session
            self.spark = self._create_spark_session()
            
            # Setup database properties
            self.db_properties = {
                "user": self.db_user,
                "password": self.db_password,
                "driver": "org.postgresql.Driver"
            }
            
            # Define schema
            self.schema = self.define_schema()
            self.logger.info(f"Schema defined with {len(self.schema.fields)} fields")
            
            # Validate database connection
            self._validate_database_connection()
            
            # Create table if it doesn't exist
            self.create_table_if_not_exists()
            
            # Create necessary directories
            self._ensure_directories()
            
            self.logger.info("EcommerceStreamingJob initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize EcommerceStreamingJob: {str(e)}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def _validate_configuration(self, spark_master: str, input_path: str, 
                              checkpoint_path: str, db_url: str, db_table: str, 
                              db_user: str, db_password: str):
        """Validate all configuration parameters."""
        self.logger.debug("Validating configuration parameters...")
        
        # Validate Spark master
        if not spark_master:
            raise ValueError("Spark master URL cannot be empty")
        if not spark_master.startswith(("local", "spark://", "yarn")):
            self.logger.warning(f"Unusual Spark master URL: {spark_master}")

        # Windows requires winutils.exe to avoid SparkContext initialization failures.
        # Running the driver inside Docker (Linux) also avoids this.
        if _is_windows() and not _windows_has_winutils():
            raise EnvironmentError(
                "Spark on Windows requires Hadoop winutils.exe, but neither HADOOP_HOME nor hadoop.home.dir points to a bin/winutils.exe. "
                "Recommended: run via Docker Compose (cd docker; docker compose up -d --build). "
                "If you must run locally on Windows, install winutils.exe and set HADOOP_HOME (or hadoop.home.dir) to that Hadoop home directory."
            )
        
        # Validate paths
        if not input_path:
            raise ValueError("Input path cannot be empty")
        if not checkpoint_path:
            raise ValueError("Checkpoint path cannot be empty")
            
        # Validate database configuration
        db_url = db_url or os.getenv("DATABASE_URL")
        db_user = db_user or os.getenv("POSTGRES_USER")
        db_password = db_password or os.getenv("POSTGRES_PASSWORD")
        
        if not db_url:
            raise ValueError("Database URL must be provided either as parameter or DATABASE_URL environment variable")
        if not db_user:
            raise ValueError("Database user must be provided either as parameter or POSTGRES_USER environment variable")
        if not db_password:
            raise ValueError("Database password must be provided either as parameter or POSTGRES_PASSWORD environment variable")
            
        if not db_table:
            raise ValueError("Database table name cannot be empty")
            
        self.logger.debug("Configuration validation completed")

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session with proper settings."""
        self.logger.info("Creating Spark session...")
        
        try:
            builder = (
                SparkSession.builder
                .appName("EcommerceStructuredStreaming")
                .master(self.spark_master)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.streaming.checkpointLocation.fallback", self.checkpoint_path)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            )

            # Ensure PostgreSQL JDBC driver is available to Spark.
            # In Docker images we may have a local jar; in local dev we can fall back to Maven coordinates.
            jdbc_jar = os.getenv("POSTGRES_JDBC_JAR", "/opt/spark/jars/postgresql-42.6.0.jar")
            if jdbc_jar and Path(jdbc_jar).exists():
                builder = (
                    builder
                    .config("spark.jars", jdbc_jar)
                    .config("spark.driver.extraClassPath", jdbc_jar)
                    .config("spark.executor.extraClassPath", jdbc_jar)
                )
                self.logger.info(f"Using PostgreSQL JDBC jar: {jdbc_jar}")
            else:
                packages = os.getenv("SPARK_JDBC_PACKAGES", "org.postgresql:postgresql:42.7.0")
                builder = builder.config("spark.jars.packages", packages)
                self.logger.warning(
                    f"PostgreSQL JDBC jar not found at '{jdbc_jar}'. Falling back to spark.jars.packages={packages}"
                )

            spark = builder.getOrCreate()
            
            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")
            
            self.logger.info(f"Spark session created successfully - Version: {spark.version}")
            self.logger.debug(f"Spark master: {spark.sparkContext.master}")
            self.logger.debug(f"Spark app name: {spark.sparkContext.appName}")
            
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise

    def _validate_database_connection(self):
        """Validate database connection before starting streaming."""
        self.logger.info("Validating database connection...")
        
        try:
            # Extract connection details from JDBC URL
            if "jdbc:postgresql://" not in self.db_url:
                raise ValueError(f"Invalid PostgreSQL JDBC URL: {self.db_url}")
                
            url_parts = self.db_url.replace("jdbc:postgresql://", "").split("/")
            if len(url_parts) != 2:
                raise ValueError(f"Invalid JDBC URL format: {self.db_url}")
                
            host_port = url_parts[0]
            database = url_parts[1]
            
            if ":" not in host_port:
                raise ValueError(f"Invalid host:port format: {host_port}")
                
            host, port_str = host_port.split(":")
            try:
                port = int(port_str)
            except ValueError:
                raise ValueError(f"Invalid port number: {port_str}")
            
            # Test connection
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=self.db_user,
                password=self.db_password,
                connect_timeout=10
            )
            
            # Test query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result[0] != 1:
                raise Exception("Database connection test failed")
                
            self.logger.info(f"Database connection validated successfully - Host: {host}, Port: {port}, Database: {database}")
            
        except psycopg2.Error as e:
            self.logger.error(f"Database connection validation failed: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Database connection validation error: {str(e)}")
            raise

    def _ensure_directories(self):
        """Ensure required directories exist."""
        try:
            # Create input directory if it doesn't exist
            input_dir = Path(self.input_path)
            input_dir.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Input directory ensured: {input_dir}")
            
            # Create checkpoint directory if it doesn't exist
            checkpoint_dir = Path(self.checkpoint_path)
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Checkpoint directory ensured: {checkpoint_dir}")
            
        except Exception as e:
            self.logger.error(f"Failed to create directories: {str(e)}")
            raise


    def create_table_if_not_exists(self):
        """Create PostgreSQL table with the required schema and comprehensive logging."""
        self.logger.info(f"Creating/verifying table: {self.db_table}")
        
        try:
            # Extract connection details from JDBC URL
            url_parts = self.db_url.replace("jdbc:postgresql://", "").split("/")
            host_port = url_parts[0]
            database = url_parts[1]
            host, port_str = host_port.split(":")
            port = int(port_str)
            
            self.logger.debug(f"Connecting to database: {host}:{port}/{database}")
            
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=self.db_properties["user"],
                password=self.db_properties["password"],
                connect_timeout=30
            )
            
            cursor = conn.cursor()
            
            # Check if table exists
            check_table_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """
            
            cursor.execute(check_table_query, (self.db_table,))
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                self.logger.info(f"Table {self.db_table} already exists")
                
                # Validate table schema
                self._validate_table_schema(cursor)
            else:
                self.logger.info(f"Creating table {self.db_table}")
                
                create_table_sql = f"""
                CREATE TABLE {self.db_table} (
                    event_id VARCHAR(100) PRIMARY KEY,
                    event_type VARCHAR(50),
                    product_id INTEGER,
                    product_name TEXT,
                    category VARCHAR(100),
                    brand VARCHAR(100),
                    sku VARCHAR(50),
                    price DECIMAL(10,2),
                    customer_id VARCHAR(100),
                    customer_email VARCHAR(255),
                    customer_name VARCHAR(255),
                    customer_address TEXT,
                    session_id VARCHAR(100),
                    user_agent TEXT,
                    ip_address VARCHAR(45),
                    price_category VARCHAR(20),
                    device_type VARCHAR(20),
                    event_timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Create indexes for better performance
                CREATE INDEX IF NOT EXISTS idx_event_type ON {self.db_table}(event_type);
                CREATE INDEX IF NOT EXISTS idx_product_id ON {self.db_table}(product_id);
                CREATE INDEX IF NOT EXISTS idx_category ON {self.db_table}(category);
                CREATE INDEX IF NOT EXISTS idx_customer_id ON {self.db_table}(customer_id);
                CREATE INDEX IF NOT EXISTS idx_event_timestamp ON {self.db_table}(event_timestamp);
                CREATE INDEX IF NOT EXISTS idx_created_at ON {self.db_table}(created_at);
                """
                
                cursor.execute(create_table_sql)
                self.logger.info(f"Table {self.db_table} created successfully with indexes")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Table {self.db_table} setup completed successfully")
            
        except psycopg2.Error as e:
            self.logger.error(f"Database error while creating table: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error while creating table: {str(e)}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def _validate_table_schema(self, cursor):
        """Validate that the existing table has the correct schema."""
        self.logger.debug(f"Validating schema for table {self.db_table}")
        
        try:
            # Get table columns
            schema_query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position;
            """
            
            cursor.execute(schema_query, (self.db_table,))
            existing_columns = {row[0]: {'type': row[1], 'nullable': row[2]} for row in cursor.fetchall()}
            
            # Expected columns based on our schema
            expected_columns = {
                'event_id', 'event_type', 'product_id', 'product_name',
                'category', 'brand', 'sku', 'price', 'customer_id',
                'customer_email', 'customer_name', 'customer_address',
                'session_id', 'user_agent', 'ip_address', 'price_category',
                'device_type', 'event_timestamp', 'created_at'
            }
            
            missing_columns = expected_columns - set(existing_columns.keys())
            extra_columns = set(existing_columns.keys()) - expected_columns
            
            if missing_columns:
                self.logger.warning(f"Missing columns in table: {missing_columns}")
            
            if extra_columns:
                self.logger.info(f"Extra columns in table: {extra_columns}")
                
            self.logger.debug(f"Table schema validation completed. Found {len(existing_columns)} columns")
            
        except Exception as e:
            self.logger.warning(f"Could not validate table schema: {str(e)}")

    def define_schema(self):
        """Define schema explicitly (BEST PRACTICE)."""
        return StructType([
            # Event fields
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), True),
            
            # Product fields
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("price", DoubleType(), True),
            
            # Customer fields
            StructField("customer_id", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_address", StringType(), True),
            
            # Session and technical fields
            StructField("session_id", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("ip_address", StringType(), True),
            
            # Timestamp field
            StructField("event_timestamp", TimestampType(), True),
        ])

    def read_stream(self):
        """Read CSV files as a streaming source."""
        return (
            self.spark.readStream
            .schema(self.schema)
            .option("header", "true")
            .csv(self.input_path)
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply transformations and comprehensive data quality checks."""
        self.logger.info("Starting data transformations and validation")
        
        try:
            # Log initial DataFrame info (cannot count streaming DataFrames)
            self.logger.debug(f"Processing streaming DataFrame with schema: {df.schema}")
            
            # Data cleaning and transformations
            transformed_df = (
                df
                # Convert timestamp with validation
                .withColumn(
                    "event_timestamp",
                    to_timestamp(col("event_timestamp"))
                )
                # Clean and normalize text data
                .withColumn("category", trim(col("category")))
                .withColumn("brand", trim(col("brand")))
                .withColumn("product_name", trim(col("product_name")))
                .withColumn("customer_email", lower(trim(col("customer_email"))))
                .withColumn("event_type", lower(trim(col("event_type"))))
                .withColumn("customer_name", trim(col("customer_name")))
                .withColumn("sku", trim(col("sku")))
                
                # Add derived fields with validation
                .withColumn(
                    "price_category",
                    when(col("price").isNull(), "Unknown")
                    .when(col("price") < 0, "Invalid")
                    .when(col("price") < 50, "Low")
                    .when(col("price") < 200, "Medium")
                    .otherwise("High")
                )
                .withColumn(
                    "device_type",
                    when(col("user_agent").isNull(), "Unknown")
                    .when(col("user_agent").contains("Mobile"), "Mobile")
                    .when(col("user_agent").contains("Tablet"), "Tablet")
                    .when(col("user_agent").contains("iPad"), "Tablet")
                    .when(col("user_agent").contains("Android"), "Mobile")
                    .when(col("user_agent").contains("iPhone"), "Mobile")
                    .otherwise("Desktop")
                )
            )
            
            # Data quality validations and filtering
            validated_df = self._apply_data_validation(transformed_df)
            
            self.logger.info("Data transformations and validation completed")
            return validated_df
            
        except Exception as e:
            self.logger.error(f"Error in data transformation: {str(e)}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def _apply_data_validation(self, df: DataFrame) -> DataFrame:
        """Apply comprehensive data validation rules."""
        self.logger.debug("Applying data validation rules for streaming DataFrame")
        
        try:
            # Note: Cannot count streaming DataFrames, validation applied as filters
            self.logger.debug("Applying validation filters to streaming DataFrame")
            
            # Apply validation filters
            validated_df = (
                df
                # Required field validations
                .filter(col("event_id").isNotNull())
                .filter(col("event_id") != "")
                .filter(col("product_id").isNotNull())
                .filter(col("product_id") > 0)
                .filter(col("event_type").isNotNull())
                .filter(col("event_type") != "")
                
                # Price validations
                .filter(col("price").isNotNull())
                .filter(col("price") > 0)
                .filter(col("price") <= 10000)  # Reasonable upper limit
                
                # Email validation (basic)
                .filter(col("customer_email").contains("@"))
                .filter(col("customer_email").contains("."))
                
                # Timestamp validation
                .filter(col("event_timestamp").isNotNull())
                
                # String length validations
                .filter(col("product_name").isNotNull())
                .filter(col("product_name") != "")
                .filter(col("category").isNotNull())
                .filter(col("category") != "")
            )
            
            # Note: Cannot count streaming DataFrames - validation metrics available per batch
            self.logger.info("Validation filters applied to streaming DataFrame")
            return validated_df
            
        except Exception as e:
            self.logger.error(f"Error in data validation: {str(e)}")
            raise

    def write_to_postgres(self, df):
        """Write stream to PostgreSQL using foreachBatch."""
        return (
            df.writeStream
            .foreachBatch(self._write_batch)
            .option("checkpointLocation", self.checkpoint_path)
            .outputMode("append")
            .start()
        )

    def _write_batch(self, batch_df: DataFrame, batch_id: int):
        """Write each micro-batch to PostgreSQL with comprehensive validation and logging."""
        batch_start_time = datetime.now(timezone.utc)
        
        try:
            self.logger.info(f"Processing batch {batch_id}")

            # Drop duplicate event_ids inside the batch early to reduce noise.
            # NOTE: This does not solve retry/partial-write duplicates; the sink must be idempotent.
            batch_df = batch_df.dropDuplicates(["event_id"])
            
            # Count records
            record_count = batch_df.count()
            
            if record_count == 0:
                self.logger.info(f"Batch {batch_id}: No records to process")
                return
            
            self.logger.info(f"Batch {batch_id}: Processing {record_count} records")
            
            # Validate batch data quality
            validation_results = self._validate_batch_quality(batch_df, batch_id)
            
            if not validation_results['is_valid']:
                self.logger.error(f"Batch {batch_id}: Failed quality validation")
                self.total_records_failed += record_count
                return
            
            # Show sample data for monitoring (if debug enabled)
            if self.logger.isEnabledFor(logging.DEBUG) and record_count > 0:
                self.logger.debug(f"Sample data from batch {batch_id}:")
                try:
                    sample_data = batch_df.select(
                        "event_type", "product_name", "category", 
                        "brand", "price", "device_type", "price_category"
                    ).limit(3).collect()
                    
                    for i, row in enumerate(sample_data):
                        self.logger.debug(f"  Sample {i+1}: {row.asDict()}")
                        
                except Exception as e:
                    self.logger.warning(f"Could not collect sample data: {str(e)}")
            
            # Log batch statistics
            self._log_batch_statistics(batch_df, batch_id)
            
            # Write to PostgreSQL with retry logic
            success = self._write_to_postgres_with_retry(batch_df, batch_id)
            
            if success:
                batch_duration = (datetime.now(timezone.utc) - batch_start_time).total_seconds()
                self.total_batches_processed += 1
                self.total_records_processed += record_count
                
                self.logger.info(
                    f"Batch {batch_id}: Successfully wrote {record_count} records "
                    f"in {batch_duration:.2f}s ({record_count/batch_duration:.2f} records/sec)"
                )
                
                # Log periodic summary statistics
                if self.total_batches_processed % 10 == 0:
                    self._log_periodic_statistics()
            else:
                self.total_records_failed += record_count
                
        except Exception as e:
            self.logger.error(f"Critical error in batch {batch_id}: {str(e)}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            self.total_records_failed += batch_df.count() if batch_df else 0
            raise

    def _validate_batch_quality(self, batch_df: DataFrame, batch_id: int) -> Dict[str, Any]:
        """Validate batch data quality and return detailed results."""
        try:
            self.logger.debug(f"Validating quality for batch {batch_id}")
            
            # Check for required columns
            expected_columns = {
                'event_id', 'event_type', 'product_id', 'product_name',
                'category', 'brand', 'sku', 'price', 'customer_id',
                'customer_email', 'customer_name', 'customer_address',
                'session_id', 'user_agent', 'ip_address', 'event_timestamp'
            }
            
            actual_columns = set(batch_df.columns)
            missing_columns = expected_columns - actual_columns
            
            if missing_columns:
                self.logger.error(f"Batch {batch_id}: Missing columns: {missing_columns}")
                return {'is_valid': False, 'error': f'Missing columns: {missing_columns}'}
            
            # Check for null values in critical fields
            critical_fields = ['event_id', 'product_id', 'price', 'event_timestamp']
            null_counts = {}
            
            for field in critical_fields:
                null_count = batch_df.filter(col(field).isNull()).count()
                if null_count > 0:
                    null_counts[field] = null_count
            
            if null_counts:
                self.logger.warning(f"Batch {batch_id}: Null values found: {null_counts}")
            
            # Check for duplicate event_ids
            total_count = batch_df.count()
            unique_count = batch_df.select("event_id").distinct().count()
            
            if total_count != unique_count:
                duplicates = total_count - unique_count
                self.logger.warning(f"Batch {batch_id}: Found {duplicates} duplicate event_ids")
            
            # Perform robust price aggregations using Spark functions
            try:
                agg_row = batch_df.agg(
                    spark_min(col("price")).alias("min_price"),
                    spark_max(col("price")).alias("max_price"),
                    spark_avg(col("price")).alias("avg_price")
                ).collect()

                if not agg_row:
                    self.logger.warning(f"Batch {batch_id}: Price aggregation returned no rows")
                    return {'is_valid': False, 'error': 'Empty aggregation result'}

                stats = agg_row[0].asDict()
                min_price = stats.get('min_price')
                max_price = stats.get('max_price')
                avg_price = stats.get('avg_price')

                # Handle None results gracefully
                if min_price is None or max_price is None:
                    self.logger.warning(f"Batch {batch_id}: Price aggregation returned None values: {stats}")
                    return {'is_valid': False, 'error': 'Price aggregation returned None', 'price_stats': stats}

                # Basic sanity checks
                if min_price < 0:
                    self.logger.error(f"Batch {batch_id}: Found negative min price: {min_price}")
                    return {'is_valid': False, 'price_stats': stats}

                if max_price > 10000:
                    self.logger.error(f"Batch {batch_id}: Found implausible max price: {max_price}")
                    return {'is_valid': False, 'price_stats': stats}

            except Exception as e:
                self.logger.error(f"Batch {batch_id}: Error computing price aggregations: {str(e)}")
                self.logger.debug(f"Traceback: {traceback.format_exc()}")
                return {'is_valid': False, 'error': str(e)}

            return {
                'is_valid': True,
                'null_counts': null_counts,
                'duplicate_count': total_count - unique_count,
                'price_stats': {
                    'min': float(min_price),
                    'max': float(max_price),
                    'avg': float(avg_price) if avg_price is not None else None
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error validating batch {batch_id}: {str(e)}")
            return {'is_valid': False, 'error': str(e)}

    def _log_batch_statistics(self, batch_df: DataFrame, batch_id: int):
        """Log detailed batch statistics."""
        try:
            if not self.logger.isEnabledFor(logging.INFO):
                return
                
            self.logger.debug(f"Generating statistics for batch {batch_id}")
            
            # Event type distribution
            event_type_stats = batch_df.groupBy("event_type").count().collect()
            event_type_dist = {row["event_type"]: row["count"] for row in event_type_stats}
            
            # Category distribution
            category_stats = batch_df.groupBy("category").count().collect()
            category_dist = {row["category"]: row["count"] for row in category_stats}
            
            # Device type distribution
            device_stats = batch_df.groupBy("device_type").count().collect()
            device_dist = {row["device_type"]: row["count"] for row in device_stats}
            
            self.logger.info(f"Batch {batch_id} statistics:")
            self.logger.info(f"  Event types: {event_type_dist}")
            self.logger.info(f"  Categories: {category_dist}")
            self.logger.info(f"  Devices: {device_dist}")
            
        except Exception as e:
            self.logger.warning(f"Could not generate statistics for batch {batch_id}: {str(e)}")

    def _write_to_postgres_with_retry(self, batch_df: DataFrame, batch_id: int, max_retries: int = 3) -> bool:
        """Write to PostgreSQL with retry logic."""
        
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Writing batch {batch_id} to PostgreSQL (attempt {attempt + 1})")

                # JDBC sinks are not exactly-once. If a retry happens after partial inserts,
                # naive append will hit primary-key conflicts and the whole batch can fail.
                # To make the write idempotent, write the batch to a staging table and then
                # merge into the target with ON CONFLICT DO NOTHING.
                staging_table = self._staging_table_name(batch_id)

                (
                    batch_df.write
                    .jdbc(
                        url=self.db_url,
                        table=staging_table,
                        mode="overwrite",
                        properties=self.db_properties,
                    )
                )

                self._merge_staging_into_target(staging_table, batch_df.columns)
                self._drop_table_if_exists(staging_table)
                
                return True
                
            except Exception as e:
                self.logger.warning(
                    f"Attempt {attempt + 1} failed for batch {batch_id}: {str(e)}"
                )
                
                if attempt == max_retries - 1:
                    self.logger.error(
                        f"All {max_retries} attempts failed for batch {batch_id}. "
                        f"Final error: {str(e)}"
                    )
                    return False
                else:
                    # Wait before retry
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        return False

    def _log_periodic_statistics(self):
        """Log periodic summary statistics."""
        runtime = datetime.now(timezone.utc) - self.start_time
        
        self.logger.info(
            f"=== PERIODIC SUMMARY (Runtime: {runtime}) ==="
        )
        self.logger.info(f"Total batches processed: {self.total_batches_processed}")
        self.logger.info(f"Total records processed: {self.total_records_processed}")
        self.logger.info(f"Total records failed: {self.total_records_failed}")
        
        if self.total_records_processed > 0:
            success_rate = ((self.total_records_processed / 
                           (self.total_records_processed + self.total_records_failed)) * 100)
            self.logger.info(f"Success rate: {success_rate:.2f}%")
            
        if runtime.total_seconds() > 0:
            avg_records_per_sec = self.total_records_processed / runtime.total_seconds()
            self.logger.info(f"Average records/second: {avg_records_per_sec:.2f}")
        
        self.logger.info("=== END SUMMARY ===")

    def _parse_jdbc_url(self) -> tuple[str, int, str]:
        """Parse a Postgres JDBC URL like jdbc:postgresql://host:5432/db into components."""
        if "jdbc:postgresql://" not in (self.db_url or ""):
            raise ValueError(f"Invalid PostgreSQL JDBC URL: {self.db_url}")

        url_parts = self.db_url.replace("jdbc:postgresql://", "").split("/")
        if len(url_parts) != 2:
            raise ValueError(f"Invalid JDBC URL format: {self.db_url}")

        host_port = url_parts[0]
        database = url_parts[1]

        if ":" not in host_port:
            raise ValueError(f"Invalid host:port format: {host_port}")

        host, port_str = host_port.split(":", 1)
        return host, int(port_str), database

    def _split_table_name(self, table: str) -> tuple[str, str]:
        """Return (schema, table) from possibly schema-qualified input."""
        if "." in table:
            schema, tbl = table.split(".", 1)
            return schema, tbl
        return "public", table

    def _staging_table_name(self, batch_id: int) -> str:
        schema, tbl = self._split_table_name(self.db_table)
        # Keep it deterministic for easy cleanup; include batch_id to avoid collisions.
        return f"{schema}.{tbl}_staging_{batch_id}"

    def _drop_table_if_exists(self, table: str) -> None:
        schema, tbl = self._split_table_name(table)
        host, port, database = self._parse_jdbc_url()

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=self.db_user,
            password=self.db_password,
            connect_timeout=10,
        )
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{};").format(
                        sql.Identifier(schema),
                        sql.Identifier(tbl),
                    )
                )
        finally:
            conn.close()

    def _merge_staging_into_target(self, staging_table: str, columns: list[str]) -> None:
        target_schema, target_tbl = self._split_table_name(self.db_table)
        staging_schema, staging_tbl = self._split_table_name(staging_table)
        host, port, database = self._parse_jdbc_url()

        # Ensure event_id is part of the merge (required for conflict handling)
        if "event_id" not in columns:
            raise ValueError("Cannot merge batch: 'event_id' column missing")

        column_identifiers = [sql.Identifier(c) for c in columns]
        cols_sql = sql.SQL(", ").join(column_identifiers)

        insert_sql = sql.SQL(
            "INSERT INTO {target} ({cols}) "
            "SELECT {cols} FROM {staging} "
            "ON CONFLICT ({pk}) DO NOTHING;"
        ).format(
            target=sql.SQL("{}.{}").format(sql.Identifier(target_schema), sql.Identifier(target_tbl)),
            staging=sql.SQL("{}.{}").format(sql.Identifier(staging_schema), sql.Identifier(staging_tbl)),
            cols=cols_sql,
            pk=sql.Identifier("event_id"),
        )

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=self.db_user,
            password=self.db_password,
            connect_timeout=10,
        )
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(insert_sql)
        finally:
            conn.close()

    def run(self):
        """Run the streaming job with comprehensive monitoring and error handling."""
        self.logger.info("Starting E-commerce Structured Streaming Job...")
        self.logger.info(f"Configuration:")
        self.logger.info(f"  - Spark Master: {self.spark_master}")
        self.logger.info(f"  - Input Path: {self.input_path}")
        self.logger.info(f"  - Checkpoint Path: {self.checkpoint_path}")
        self.logger.info(f"  - Database Table: {self.db_table}")
        self.logger.info(f"  - Database URL: {self.db_url}")
        
        query = None
        
        try:
            # Read stream
            self.logger.info("Setting up streaming source...")
            stream_df = self.read_stream()
            
            # Apply transformations
            self.logger.info("Setting up data transformations...")
            transformed_df = self.transform(stream_df)
            
            # Start writing to PostgreSQL
            self.logger.info("Starting streaming query...")
            query = self.write_to_postgres(transformed_df)
            
            self.logger.info(f"Streaming query started with ID: {query.id}")
            
            # Monitor the stream
            self._monitor_stream(query)
            
            # Wait for termination
            self.logger.info("Streaming job is running. Waiting for termination...")
            query.awaitTermination()
            
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received. Initiating graceful shutdown...")
            if query:
                self._stop_query_gracefully(query)
        except Exception as e:
            self.logger.error(f"Streaming job failed with error: {str(e)}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            if query:
                self._stop_query_gracefully(query)
            raise
        finally:
            self._cleanup_resources(query)

    def _monitor_stream(self, query):
        """Monitor streaming query in background."""
        try:
            import threading
            import time
            
            def monitor():
                while query.isActive:
                    try:
                        time.sleep(30)  # Monitor every 30 seconds
                        if query.isActive:
                            self._log_stream_status(query)
                    except Exception as e:
                        self.logger.warning(f"Error in stream monitoring: {str(e)}")
                        break
            
            monitor_thread = threading.Thread(target=monitor, daemon=True)
            monitor_thread.start()
            
        except Exception as e:
            self.logger.warning(f"Could not start stream monitoring: {str(e)}")

    def _log_stream_status(self, query):
        """Log current streaming query status."""
        try:
            if not query.isActive:
                return
                
            status = query.status
            progress = query.lastProgress
            
            self.logger.info(f"Stream Status - ID: {status.id}, Active: {status.isDataAvailable}")
            
            if progress:
                batch_id = progress.get('batchId', 'N/A')
                input_rows = progress.get('inputRowsPerSecond', 0)
                processed_rows = progress.get('processedRowsPerSecond', 0)
                
                self.logger.info(
                    f"Latest Progress - Batch: {batch_id}, "
                    f"Input: {input_rows:.2f} rows/sec, "
                    f"Processed: {processed_rows:.2f} rows/sec"
                )
                
        except Exception as e:
            self.logger.debug(f"Error getting stream status: {str(e)}")

    def _stop_query_gracefully(self, query):
        """Stop streaming query gracefully."""
        try:
            if query and query.isActive:
                self.logger.info("Stopping streaming query...")
                query.stop()
                
                # Wait for query to stop
                timeout = 30
                start_time = datetime.now(timezone.utc)
                
                while query.isActive and (datetime.now(timezone.utc) - start_time).seconds < timeout:
                    import time
                    time.sleep(1)
                
                if query.isActive:
                    self.logger.warning(f"Query did not stop within {timeout} seconds")
                else:
                    self.logger.info("Streaming query stopped successfully")
                    
        except Exception as e:
            self.logger.error(f"Error stopping query: {str(e)}")

    def _cleanup_resources(self, query):
        """Cleanup resources and log final statistics."""
        try:
            self.logger.info("Cleaning up resources...")
            
            # Log final statistics
            self._log_final_statistics()
            
            # Stop Spark session
            if hasattr(self, 'spark') and self.spark:
                self.logger.info("Stopping Spark session...")
                self.spark.stop()
                self.logger.info("Spark session stopped")
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    def _log_final_statistics(self):
        """Log comprehensive final statistics."""
        runtime = datetime.now(timezone.utc) - self.start_time
        
        stats_msg = f"""
=== SPARK STREAMING JOB FINAL STATISTICS ===
Runtime: {runtime}
Total Batches Processed: {self.total_batches_processed}
Total Records Processed: {self.total_records_processed}
Total Records Failed: {self.total_records_failed}
Success Rate: {((self.total_records_processed / max(self.total_records_processed + self.total_records_failed, 1)) * 100):.2f}%
Average Records/Second: {(self.total_records_processed / max(runtime.total_seconds(), 1)):.2f}
Average Batches/Minute: {(self.total_batches_processed / max(runtime.total_seconds()/60, 1)):.2f}
Input Path: {self.input_path}
Output Table: {self.db_table}
=== END STATISTICS ===
        """
        
        self.logger.info(stats_msg)
        print(stats_msg)  # Also print to console for visibility

def main():
    """Main function with comprehensive configuration and error handling."""
    try:
        # Configuration from environment variables with defaults
        spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        input_path = os.getenv("INPUT_DATA_DIR", "/app/data/incoming")
        checkpoint_path = os.getenv("CHECKPOINT_DIR", "/app/data/checkpoints")
        db_table = os.getenv("DB_TABLE", "ecommerce_events")
        log_level = os.getenv("LOG_LEVEL", "INFO")
        
        print(f"Starting Spark Streaming Job with configuration:")
        print(f"  - Spark Master: {spark_master}")
        print(f"  - Input Path: {input_path}")
        print(f"  - Checkpoint Path: {checkpoint_path}")
        print(f"  - Database Table: {db_table}")
        print(f"  - Log Level: {log_level}")
        
        # Create and run the streaming job
        job = EcommerceStreamingJob(
            spark_master=spark_master,
            input_path=input_path,
            checkpoint_path=checkpoint_path,
            db_table=db_table,
            log_level=log_level
        )
        
        job.run()
        
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error in main: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
