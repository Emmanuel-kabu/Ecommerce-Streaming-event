""" Script to generate synthetic e-commerce transaction data and stream it to a PostgreSQL database. 
    The data includes products, orders, and customer information.
    The script uses Faker to create realistic data and SQLAlchemy to interact with the database.
    The data generation runs in an infinite loop, continuously adding new transactions at specified intervals.
    with proper error handling and logging for monitoring the data generation process.
 
       """
import os
import sys
import time
import random
import logging
import signal
import traceback
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from faker import Faker
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_logging(log_level: str = "INFO", log_dir: str = "logs") -> logging.Logger:
    """Setup comprehensive logging with file and console handlers."""
    
    # Create logs directory
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # Create logger
    logger = logging.getLogger("EcommerceDataGenerator")
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
    
    # File handler (rotating daily)
    log_filename = log_path / f"data_generator_{datetime.now().strftime('%Y%m%d')}.log"
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


class EcommerceEventGenerator:
    """
    Generate synthetic e-commerce events at a fixed interval
    and write them as CSV files for Spark Structured Streaming.
    """

    def __init__(
        self,
        interval_seconds: int = 5,
        output_dir: str | None = None,
        event_per_batch: int = 100,
        log_level: str = "INFO"
    ):
        # Setup logging first
        self.logger = setup_logging(log_level, "logs")
        
        self.logger.info("Initializing EcommerceEventGenerator...")
        self.logger.debug(f"Configuration: interval={interval_seconds}s, output_dir={output_dir}, events_per_batch={event_per_batch}")
        
        self.interval_seconds = interval_seconds
        # Allow overriding output directory via environment variable HOST_DATA_DIR
        # Default to the Docker-mounted data folder relative to this package: ../data/incoming
        env_output = os.getenv("HOST_DATA_DIR") or output_dir
        self.output_dir = env_output or os.path.join("..", "data", "incoming")
        self.event_per_batch = event_per_batch
        
        # Statistics tracking
        self.total_events_generated = 0
        self.total_batches_processed = 0
        self.total_files_created = 0
        self.start_time = datetime.utcnow()
        self.errors_count = 0

        try:
            self.faker = Faker()
            self.logger.debug("Faker instance initialized")
            
            self.event_types = [
                "view", "order", "cart", "click",
                "purchase", "add_to_wishlist"
            ]
            self.logger.debug(f"Event types configured: {self.event_types}")

            # Define product categories and realistic product names
            self.product_categories = self._initialize_product_categories()
            self.logger.info(f"Product categories initialized: {list(self.product_categories.keys())}")

            # Pre-generate products
            self.products = self.generate_products(50)
            self.logger.info(f"Generated {len(self.products)} products across {len(self.product_categories)} categories")

            # Create output directory
            self._ensure_output_directory()
            
            # Setup signal handlers for graceful shutdown
            self._setup_signal_handlers()
            
            self.logger.info("EcommerceEventGenerator initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize EcommerceEventGenerator: {str(e)}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def _initialize_product_categories(self) -> Dict[str, List[str]]:
        """Initialize product categories with validation."""
        categories = {
            "Electronics": [
                "Wireless Bluetooth Headphones", "Smart LED TV", "Gaming Laptop", 
                "Smartphone Case", "Wireless Charger", "Bluetooth Speaker", 
                "Digital Camera", "Smart Watch", "Tablet", "USB Cable"
            ],
            "Clothing": [
                "Cotton T-Shirt", "Denim Jeans", "Running Shoes", "Winter Jacket", 
                "Baseball Cap", "Leather Belt", "Polo Shirt", "Sneakers", 
                "Hoodie", "Dress Shirt"
            ],
            "Home & Garden": [
                "Coffee Maker", "Vacuum Cleaner", "Garden Hose", "LED Lamp", 
                "Throw Pillow", "Plant Pot", "Kitchen Knife Set", "Bedsheet Set", 
                "Wall Clock", "Storage Box"
            ],
            "Books": [
                "Programming Guide", "Cooking Cookbook", "Travel Journal", 
                "Mystery Novel", "Science Textbook", "Self-Help Book", 
                "Historical Biography", "Art Book", "Children's Story", "Dictionary"
            ],
            "Sports": [
                "Yoga Mat", "Tennis Racket", "Basketball", "Running Shoes", 
                "Gym Bag", "Water Bottle", "Resistance Bands", "Dumbbells", 
                "Soccer Ball", "Bike Helmet"
            ]
        }
        
        # Validate categories
        total_products = sum(len(products) for products in categories.values())
        self.logger.debug(f"Total product templates available: {total_products}")
        
        for category, products in categories.items():
            self.logger.debug(f"Category '{category}': {len(products)} products")
            if len(products) == 0:
                self.logger.warning(f"Category '{category}' has no products defined")
        
        return categories

    def _ensure_output_directory(self):
        """Create output directory with proper error handling."""
        try:
            output_path = Path(self.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Test write permissions
            test_file = output_path / ".test_write"
            test_file.write_text("test")
            test_file.unlink()
            
            self.logger.info(f"Output directory verified: {output_path.absolute()}")
            
        except PermissionError:
            self.logger.error(f"Permission denied: Cannot write to {self.output_dir}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to create output directory {self.output_dir}: {str(e)}")
            raise

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}. Shutting down gracefully...")
            self.print_statistics()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def generate_products(self, num_products: int) -> List[Dict[str, Any]]:
        """Generate a list of realistic products with categories and extensive validation."""
        self.logger.info(f"Generating {num_products} products...")
        
        products = []
        product_id = 1
        generated_skus = set()  # Track SKUs to avoid duplicates
        
        try:
            for category, product_names in self.product_categories.items():
                self.logger.debug(f"Processing category: {category} with {len(product_names)} product templates")
                
                for product_name in product_names:
                    if product_id > num_products:
                        self.logger.debug(f"Reached target of {num_products} products")
                        break
                        
                    try:
                        # Generate realistic price ranges based on category
                        price_ranges = {
                            "Electronics": (50.0, 2000.0),
                            "Clothing": (10.0, 200.0), 
                            "Home & Garden": (15.0, 500.0),
                            "Books": (5.0, 50.0),
                            "Sports": (20.0, 300.0)
                        }
                        
                        min_price, max_price = price_ranges.get(category, (10.0, 100.0))
                        price = round(random.uniform(min_price, max_price), 2)
                        
                        # Add some variation to product names using Faker
                        variations = [
                            product_name,
                            f"{self.faker.company()} {product_name}",
                            f"Premium {product_name}",
                            f"{product_name} - {self.faker.color_name()} Edition"
                        ]
                        
                        final_name = random.choice(variations)
                        brand = self.faker.company()
                        
                        # Generate unique SKU
                        base_sku = f"{category[:3].upper()}-{product_id:04d}"
                        sku = base_sku
                        counter = 1
                        while sku in generated_skus:
                            sku = f"{base_sku}-{counter}"
                            counter += 1
                        generated_skus.add(sku)
                        
                        product = {
                            "product_id": product_id,
                            "product_name": final_name,
                            "category": category,
                            "price": price,
                            "brand": brand,
                            "sku": sku
                        }
                        
                        # Validate product data
                        if self._validate_product(product):
                            products.append(product)
                            self.logger.debug(f"Generated product {product_id}: {final_name[:30]}...")
                        else:
                            self.logger.warning(f"Invalid product generated: {product}")
                            
                        product_id += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error generating product {product_id}: {str(e)}")
                        self.errors_count += 1
                        continue
                        
        except Exception as e:
            self.logger.error(f"Critical error in product generation: {str(e)}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            raise
            
        final_products = products[:num_products]
        self.logger.info(f"Successfully generated {len(final_products)} valid products")
        
        # Log category distribution
        category_counts = {}
        for product in final_products:
            cat = product['category']
            category_counts[cat] = category_counts.get(cat, 0) + 1
        
        for category, count in category_counts.items():
            self.logger.debug(f"Category '{category}': {count} products")
            
        return final_products

    def _validate_product(self, product: Dict[str, Any]) -> bool:
        """Validate product data integrity."""
        required_fields = ["product_id", "product_name", "category", "price", "brand", "sku"]
        
        for field in required_fields:
            if field not in product:
                self.logger.error(f"Missing required field '{field}' in product")
                return False
            if product[field] is None or product[field] == "":
                self.logger.error(f"Empty value for required field '{field}' in product")
                return False
        
        # Validate price
        if not isinstance(product["price"], (int, float)) or product["price"] <= 0:
            self.logger.error(f"Invalid price: {product['price']}")
            return False
            
        # Validate product_id
        if not isinstance(product["product_id"], int) or product["product_id"] <= 0:
            self.logger.error(f"Invalid product_id: {product['product_id']}")
            return False
            
        return True

    def generate_event(self) -> Dict[str, Any]:
        """Generate a single e-commerce event with validation."""
        try:
            if not self.products:
                raise ValueError("No products available for event generation")
            
            product = random.choice(self.products)
            event_type = random.choice(self.event_types)
            
            # Generate customer data
            customer_id = str(self.faker.uuid4())
            customer_email = self.faker.email()
            customer_name = self.faker.name()
            customer_address = self.faker.address().replace('\n', ', ')
            
            # Generate session data
            session_id = str(self.faker.uuid4())
            user_agent = self.faker.user_agent()
            ip_address = self.faker.ipv4()
            
            # Generate event data
            event_id = str(self.faker.uuid4())
            timestamp = datetime.utcnow().isoformat()
            
            event = {
                "event_id": event_id,
                "event_type": event_type,
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "brand": product["brand"],
                "sku": product["sku"],
                "price": product["price"],
                "customer_id": customer_id,
                "customer_email": customer_email,
                "customer_name": customer_name,
                "customer_address": customer_address,
                "session_id": session_id,
                "user_agent": user_agent,
                "ip_address": ip_address,
                "event_timestamp": timestamp
            }
            
            # Validate event
            if self._validate_event(event):
                return event
            else:
                raise ValueError("Generated invalid event")
                
        except Exception as e:
            self.logger.error(f"Error generating event: {str(e)}")
            self.errors_count += 1
            raise

    def _validate_event(self, event: Dict[str, Any]) -> bool:
        """Validate event data integrity."""
        required_fields = [
            "event_id", "event_type", "product_id", "product_name",
            "category", "brand", "sku", "price", "customer_id",
            "customer_email", "customer_name", "customer_address",
            "session_id", "user_agent", "ip_address", "event_timestamp"
        ]
        
        for field in required_fields:
            if field not in event or event[field] is None:
                self.logger.error(f"Missing or null field '{field}' in event")
                return False
                
        # Validate specific field types and formats
        try:
            # Validate timestamp
            datetime.fromisoformat(event["event_timestamp"].replace('Z', '+00:00'))
            
            # Validate email format
            if '@' not in event["customer_email"]:
                self.logger.error(f"Invalid email format: {event['customer_email']}")
                return False
                
            # Validate event type
            if event["event_type"] not in self.event_types:
                self.logger.error(f"Invalid event type: {event['event_type']}")
                return False
                
        except Exception as e:
            self.logger.error(f"Event validation failed: {str(e)}")
            return False
            
        return True

    def generate_events_batch(self) -> List[Dict[str, Any]]:
        """Generate a batch of events with error handling."""
        self.logger.debug(f"Generating batch of {self.event_per_batch} events")
        events = []
        failed_events = 0
        
        for i in range(self.event_per_batch):
            try:
                event = self.generate_event()
                events.append(event)
            except Exception as e:
                failed_events += 1
                self.logger.warning(f"Failed to generate event {i+1}/{self.event_per_batch}: {str(e)}")
                
                # If too many events fail, stop the batch
                if failed_events > self.event_per_batch * 0.5:  # More than 50% failure rate
                    self.logger.error(f"High failure rate in batch generation. Failed: {failed_events}/{i+1}")
                    break
        
        if failed_events > 0:
            self.logger.warning(f"Batch generation completed with {failed_events} failed events")
        
        self.logger.debug(f"Successfully generated {len(events)} events in batch")
        return events

    def write_to_csv(self, events: List[Dict[str, Any]]) -> str:
        """Write events to a CSV file with comprehensive error handling."""
        if not events:
            self.logger.warning("No events to write - empty batch")
            return ""
            
        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]  # Include milliseconds
            filename = f"ecommerce_events_{timestamp}.csv"
            filepath = Path(self.output_dir) / filename
            
            self.logger.debug(f"Writing {len(events)} events to {filepath}")
            
            # Create DataFrame and validate
            df = pd.DataFrame(events)
            
            # Check for required columns
            expected_columns = [
                "event_id", "event_type", "product_id", "product_name",
                "category", "brand", "sku", "price", "customer_id",
                "customer_email", "customer_name", "customer_address",
                "session_id", "user_agent", "ip_address", "event_timestamp"
            ]
            
            missing_columns = set(expected_columns) - set(df.columns)
            if missing_columns:
                self.logger.error(f"Missing columns in DataFrame: {missing_columns}")
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Check for data quality issues
            null_counts = df.isnull().sum()
            if null_counts.any():
                self.logger.warning(f"Found null values: {null_counts[null_counts > 0].to_dict()}")
            
            # Write to CSV
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            # Verify file was created and has content
            if not filepath.exists():
                raise FileNotFoundError(f"File was not created: {filepath}")
                
            file_size = filepath.stat().st_size
            if file_size == 0:
                raise ValueError(f"Created file is empty: {filepath}")
            
            self.logger.info(f"Successfully wrote {len(events)} events to {filepath} ({file_size} bytes)")
            
            # Update statistics
            self.total_events_generated += len(events)
            self.total_files_created += 1
            
            return str(filepath)
            
        except Exception as e:
            self.logger.error(f"Failed to write events to CSV: {str(e)}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            self.errors_count += 1
            raise

    def print_statistics(self):
        """Print comprehensive statistics."""
        runtime = datetime.utcnow() - self.start_time
        
        stats_msg = f"""
=== E-COMMERCE DATA GENERATOR STATISTICS ===
Runtime: {runtime}
Total Batches Processed: {self.total_batches_processed}
Total Events Generated: {self.total_events_generated}
Total Files Created: {self.total_files_created}
Total Errors: {self.errors_count}
Events per Batch: {self.event_per_batch}
Generation Interval: {self.interval_seconds}s
Average Events/Second: {self.total_events_generated / max(runtime.total_seconds(), 1):.2f}
Error Rate: {(self.errors_count / max(self.total_events_generated, 1) * 100):.2f}%
Output Directory: {Path(self.output_dir).absolute()}
=== END STATISTICS ===
        """
        
        self.logger.info(stats_msg)
        print(stats_msg)

    
    def run(self):
        """Continuously generate data with comprehensive monitoring and error handling."""
        self.logger.info("Starting e-commerce event generator main loop...")
        
        try:
            while True:
                batch_start_time = datetime.utcnow()
                
                try:
                    self.logger.debug(f"Starting batch {self.total_batches_processed + 1}")
                    
                    # Generate events batch
                    events = self.generate_events_batch()
                    
                    if not events:
                        self.logger.warning("No events generated in this batch, skipping...")
                        time.sleep(self.interval_seconds)
                        continue
                    
                    # Write to CSV
                    filepath = self.write_to_csv(events)
                    
                    # Update statistics
                    self.total_batches_processed += 1
                    
                    batch_duration = (datetime.utcnow() - batch_start_time).total_seconds()
                    
                    # Log batch completion
                    self.logger.info(
                        f"Batch {self.total_batches_processed} completed: "
                        f"{len(events)} events in {batch_duration:.2f}s → {Path(filepath).name}"
                    )
                    
                    # Log periodic statistics
                    if self.total_batches_processed % 10 == 0:
                        self.logger.info(
                            f"Periodic stats: {self.total_batches_processed} batches, "
                            f"{self.total_events_generated} total events, "
                            f"{self.errors_count} errors"
                        )
                    
                    # Sleep until next batch
                    self.logger.debug(f"Sleeping for {self.interval_seconds} seconds...")
                    time.sleep(self.interval_seconds)
                    
                except KeyboardInterrupt:
                    self.logger.info("Received interrupt signal")
                    break
                except Exception as e:
                    self.logger.error(f"Error in batch {self.total_batches_processed + 1}: {str(e)}")
                    self.logger.debug(f"Traceback: {traceback.format_exc()}")
                    self.errors_count += 1
                    
                    # Sleep before retrying
                    self.logger.info(f"Waiting {self.interval_seconds} seconds before retry...")
                    time.sleep(self.interval_seconds)
                    
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received, shutting down...")
        except Exception as e:
            self.logger.critical(f"Critical error in main loop: {str(e)}")
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
        finally:
            self.logger.info("Data generator shutdown initiated")
            self.print_statistics()
            self.logger.info("Data generator shutdown complete")


def main():
    """Main function with comprehensive configuration and error handling."""
    try:
        # Configuration from environment variables with defaults
        interval_seconds = int(os.getenv("DATA_GENERATION_INTERVAL", "5"))
        # Default to the Docker-mounted host folder so Spark can see generated files
        output_dir = os.getenv("OUTPUT_DATA_DIR", "../data/incoming")
        events_per_batch = int(os.getenv("EVENTS_PER_BATCH", "100"))
        log_level = os.getenv("LOG_LEVEL", "INFO")
        
        print(f"Starting E-commerce Data Generator...")
        print(f"Configuration:")
        print(f"  - Interval: {interval_seconds}s")
        print(f"  - Output directory: {output_dir}")
        print(f"  - Events per batch: {events_per_batch}")
        print(f"  - Log level: {log_level}")
        
        generator = EcommerceEventGenerator(
            interval_seconds=interval_seconds,
            output_dir=output_dir,
            event_per_batch=events_per_batch,
            log_level=log_level
        )
        
        generator.run()
        
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

    





