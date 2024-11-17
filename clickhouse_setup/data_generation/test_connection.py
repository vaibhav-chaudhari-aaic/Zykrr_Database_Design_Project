import uuid
import random
from datetime import datetime, timedelta
import clickhouse_driver
import json
from faker import Faker

# Initialize Faker for realistic data
fake = Faker()

# ClickHouse Docker connection
client = clickhouse_driver.Client(
    host='localhost',
    port=8000,  # Default ClickHouse port
    user='default',  # Default username
    password='',     # Add password if configured
    database='zykrr_analytics_2'  # Your database name
)




# Test connection
try:
    result = client.execute('SELECT 1')
    print("Successfully connected to ClickHouse!")
    print(f"Test query result: {result}")
except Exception as e:
    print(f"Connection failed: {str(e)}")