try:
    import os
    import re
    import json
    import time
    import traceback
    from fastapi import HTTPException
    import snowflake.connector
    from dotenv import load_dotenv
    #from logger_module import setup_logger
    import logging
    from snowflake.sqlalchemy import URL
    import snowflake.connector
    import sqlalchemy.pool as pool
    from sqlalchemy.pool import QueuePool
    from sqlalchemy import create_engine
    from sqlalchemy import exc
    from datetime import datetime
    from decimal import Decimal
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    import streamlit as st
    from openai import AzureOpenAI
    #from prompts import get_system_prompt
    from dotenv import load_dotenv
except Exception as e:
    st.write("####Import Error")
    st.write("Error in import", exc_info=True)
    st.write(e)

# Load environment variables from .env file if present
load_dotenv()

sf_key = os.getenv("SNOWFLAKE_PRIVATE_KEY")

private_key_encoded = sf_key.encode()

### Encode the private key passphrase
private_key_passphrase = os.getenv("daas_edp_sf_key_passphrase")
private_key_passphrase_encoded = private_key_passphrase.encode()

### Load the private key, leveraging passphrase if needed
private_key_loaded = serialization.load_pem_private_key(
    private_key_encoded,
    password=private_key_passphrase_encoded,
    backend=default_backend(),
)

## Serialize loaded private key
private_key_serialized = private_key_loaded.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# For DAAS SF TARGET
# Fetch Snowflake Configurations from environment variables
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_PRIVATE_KEY = private_key_serialized #os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_TABLE = os.getenv('SNOWFLAKE_TABLE')

# Function to get a raw Snowflake connection
def get_conn():
    try:
        return snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
            private_key=SNOWFLAKE_PRIVATE_KEY,            
            client_session_keep_alive=True
        )
    except Exception as e:
        st.write(f"Error establishing Snowflake connection: {str(e)}")
        raise

# For Local
# Fetch Snowflake Configurations from environment variables
# SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
# SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
# SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
# SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
# SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
# SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
# SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
# Function to get a raw Snowflake connection
# def get_conn():
#     try:
#         return snowflake.connector.connect(
#             user=SNOWFLAKE_USER,
#             password=SNOWFLAKE_PASSWORD,
#             account=SNOWFLAKE_ACCOUNT,
#             warehouse=SNOWFLAKE_WAREHOUSE,
#             database=SNOWFLAKE_DATABASE,
#             schema=SNOWFLAKE_SCHEMA,
#             role=SNOWFLAKE_ROLE,
#         )
#     except Exception as e:
#         logger.error(f"Error establishing Snowflake connection: {str(e)}")
#         raise

# Create a QueuePool with the provided specifications
pool = QueuePool(get_conn, max_overflow=int(os.getenv('SF_POOL_MAX_OVERFLOW')), pool_size=int(os.getenv('SF_POOL_SIZE')), timeout=float(os.getenv('SF_POOL_TIMEOUT')))

def get_snowflake_connection():
    try:
        conn = pool.connect()
        return conn
    except Exception as e:
        st.write(f"Failed to get connection from Snowflake pool: {str(e)}")
        raise

def close_snowflake_connection(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        st.write(f"Error closing Snowflake connection: {str(e)}")

def validate_snowflake_source(dbObject):
    properties = {}
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    # Checking whether snowflake TABLE even exists in the db.
    describe_query = f""" DESCRIBE TABLE {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.{dbObject}"""
    try:
        cursor.execute(describe_query)    
        metadata = cursor.fetchall()

        column_names = [row[0] for row in metadata]
        #for column_name in column_names:
        properties[dbObject] = column_names

        cursor.close()
        return metadata
    except snowflake.connector.errors.ProgrammingError as e:            
        st.write(f"Exception occured while validating the Snowflake datasource object {dbObject}. Details: {e} \n\n")
        raise HTTPException(status_code=400, detail=f"Snowflake Object: {dbObject} doesn't exist or not authorized for access. Please check logs for more details.")
    #close_snowflake_connection(conn)

# metadata = validate_snowflake_source(SNOWFLAKE_TABLE)
# st.write(metadata)