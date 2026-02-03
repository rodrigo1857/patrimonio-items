from helpers import connect_to_db
import os
from dotenv import load_dotenv

load_dotenv()

try:
    print("Testing connection...")
    conn, cur = connect_to_db('local')
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
