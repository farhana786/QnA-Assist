import yaml
import json
import base64
import re
import uuid
import os
import streamlit as st
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex, SimpleField, SearchableField, SearchFieldDataType
)
from openai import AzureOpenAI
from snowflake_utils import get_snowflake_connection
from dotenv import load_dotenv

#Establish OpenAI connection
try:
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),  
        api_version="2024-05-01-preview",
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        )
except Exception as e:
    st.error(f"Failed to establish OpenAI connection: {e}")

deployment_name=os.getenv("DEPLOYMENT_NAME")  

# Set your Azure Blob Storage and Search service credentials
blob_service_client = BlobServiceClient.from_connection_string(os.getenv("BLOB_CONNECTION_STRING"))
container_name = os.getenv("CONTAINER_NAME")

# Define your Azure Search service endpoint and API key
endpoint = os.getenv("AISEARCH_ENDPOINT")
admin_key = os.getenv("AISEARCH_ADMIN_KEY")
index_name = os.getenv("AISEARCH_INDEX_NAME")

# Create a SearchClient instance
credential = AzureKeyCredential(admin_key)
search_client = SearchClient(endpoint=endpoint, index_name=index_name, credential=credential)

DB = "CDL_LS"
SCHEMA = "DAAS"

SCHEMA_PATH = f"{DB}.{SCHEMA}"

def get_system_prompt():
    prompt = """
You will be acting as an AI Expert named Conversational DaaS. 
You are an intelligent assistant trained to answer user prompts. The user will ask a question in user prompt, for each user prompt you should respond an answer.
Now to get started, just greet the end user.
"""
    return prompt
    
def tables_context(table_names):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    properties = {}
    table_summary =" "
    for table in table_names:
        query = f""" DESCRIBE TABLE {DB}.{SCHEMA}.{table}"""
        cursor.execute(query)
        metadata = cursor.fetchall()
        properties[table] = metadata
    tables_summary = properties
    return tables_summary

def summarize_api_spec(api_spec,filename):
    l = len(api_spec['servers'])
    summary = f"**{api_spec['info']['title']}**:\n"
    summary = f" Source file name:- {filename}**:\n"#
    for i in range(l):
        summary += f"  - URL: {api_spec['servers'][i]['url']}\n"
    summary += "  - Endpoints:\n"
    for path, methods in api_spec['paths'].items():
        for method, details in methods.items():
            summary += f"    - {method.upper()} {path}\n"
            summary += f"      - Summary: {details['summary']}\n"
            summary += f"      - Description: {details.get('description', 'No description')}\n"
            summary += "      - Parameters:\n"
            for param in details.get('parameters', []):
                summary += f"        - {param['name']} ({param['in']} - {param['schema']['type']})\n"
    return summary

GEN_API = """
You will be generating REST API HTTP URLs based on user prompts and API Specifications provided to you.
Your goal is to give correct, executable API calls and give the answer to user.
The user will ask a prompt, for each prompt you should respond and include REST API HTTP URL based on the question and the API Specifications. 
The API specifications contain the following information:
{context}
Here are 7 critical rules for the interaction you must abide:
<rules>
1. You MUST MUST wrap the generated REST API HTTP URL within ``` GET markdown in this format e.g
```GET
```
2. Extract Information: Identify the relevant endpoint, method, and parameters from the provided YAML data.
3. Match User Criteria: Ensure the generated URL includes the criteria specified by the user in their prompt.
4. Construct the URL: Combine the base URL, endpoint, and parameters to construct the full URL.
4. Make sure to generate a single REST API HTTP URL, not multiple.
5. Give answer to user prompt by fetching the answer from REST API HTTP URL generated by you.
5. You should only use the API Specifications provided to you to answer user prompts, You MUST NOT hallucinate about the API Specifications.
6. DO NOT put numerical at the very front of REST API HTTP URL.
7. Display the API response in tabular format
</rules>
    """

def get_context(api_info):
    context = " "
    for summary in api_info.values():
        context += summary + "\n"
    return context

def api_context(api_names):
    api_summaries = {}
    directory = 'D:\Conversational DaaS\export_api'
    # Iterate over each api_name
    for api_name in api_names:
        # Iterate over each filename in the directory
        for filename in os.listdir(directory):
            if api_name == filename:
                if filename.endswith('.yaml') or filename.endswith('.yml'):
                    file_path = os.path.join(directory, filename)
                    with open(file_path, 'r') as file:
                        api_spec = yaml.safe_load(file)
                        api_summaries[filename] = summarize_api_spec(api_spec, filename)
    #st.write(api_summaries)
    context = get_context(api_summaries)
    #st.write(context)
    # context_api = extract_api_summary(context)
    # st.write(context_api)
    return context 

def get_api_prompt(api_names):
    apis = api_names
    context_api = api_context(apis)
    return GEN_API.format(context=context_api)

GEN_SQL = """
You will be acting as an AI SQL Expert named Conversational DaaS.
Your goal is to give correct, executable SQL query by using the information provided to you in context and then give answer to user by executing the same query. Please include column name as headers in tabular answer.
You will be replying to users who will be confused if you don't respond in the character of QnA Assist.
The user will ask questions, for each question you should respond and include a SQL query based on the question.
The Tables description contains the following information:
{context}
Here are 10 critical rules for the interaction you must abide:
<rules>
1. You MUST MUST wrap the generated sql code within ```sql code markdown in this format e.g
```sql
```
2. Handle various SQL operations including SELECT, INSERT, UPDATE, DELETE, JOIN, aggregation, subqueries, and nested queries.
3. Use the appropriate SQL clauses (SELECT, WHERE, JOIN, GROUP BY, HAVING, etc.) to construct the SQL query
4. To retrieve information from more than one table, you need to join those tables together using JOIN methods. Use the following syntax:
SELECT <list_of_column_names>
FROM <table1>
JOIN <table2> ON <table1.column_name> = <table2.column_name>
WHERE <conditions>;

5. Use the following syntax for Complex Query with Multiple Joins:
SELECT t1.column1, t2.column2, t3.column3, ...
FROM table1 t1
JOIN table2 t2 ON t1.common_column = t2.common_column
JOIN table3 t3 ON t2.common_column = t3.common_column
    -- Add more JOIN statements as needed
WHERE t1.condition_column = condition_value AND t2.another_condition_column = another_condition_value
    -- Add more conditions as needed
GROUP BY t1.group_column, t2.group_column, ...
HAVING aggregate_function(t1.group_column) condition
ORDER BY t1.order_column ASC/DESC, t2.order_column ASC/DESC, ...

6. Use the following syntax for window function query or analytic function query:
SELECT 
    COUNT(*) OVER () AS total_count, 
    *
FROM table_name
WHERE condition_column = 'condition_value';

7. Ensure the SQL syntax is correct and adheres to the database schema and information provided to you.
8. You MUST NOT hallucinate about the tables and their metadata. Use all tables information to build a SQL query.
9. Optimize queries for performance when possible.
10. DO NOT put numerical characters at the very front of SQL variable names.
</rules>
    """

def get_tables_prompt(table_names):
    tables = table_names
    context_table = tables_context(tables)
    return GEN_SQL.format(context=context_table)

def extract_api_info_from_yaml_with_openai(api_specs,search_text):
    combined_summary = ""
    for blob_name, api_spec in api_specs:
        combined_summary += summarize_api_spec(api_spec,blob_name) + "\n"
    try:
        #print(combined_summary) 
        # end     
        prompt = f"""
        Provide an output as json format with two key one is summary and another is Sourcefile.
        1. summary will contain - steps to retrieve endpoint concatenate method details from the given YAML content of an OpenAPI specification based on the user input below. 
        Do not the fetch response details.
        The details must include at any one base URL and concatenated method details.
        It also include which api it is refering.
        Fetch all API endpoint details relevant to the user input, if available. 
        For example, if the user input is "fetch students by class,"
        provide the API that shows students by class. Additionally,
        if there are APIs to fetch related data such as classes, include those APIs as well.
        2. Sourcefile with comma separated source yaml file name.
        if there are multiple apis data is fetching make sure context from 
        the first api should maintain
        User Input: {search_text}
        """    
        #print(prompt)
        response = client.chat.completions.create(
            model=deployment_name,
            messages=[
                {"role": "system", "content": combined_summary},
                {"role": "user", "content": prompt}
            ]
        )
        extracted_info = response.choices[0].message.content
        return extracted_info
    except Exception as e:
        st.error(f"Failed to extract API info with OpenAI: {str(e)}")
        return {'message': 'Failed to extract API info with OpenAI'}, 500
    
def extract_and_index_api_spec_from_blobs(api_names,search_text):
    #api_summaries = {}
    api_specs = []
    directory = 'D:\Conversational DaaS\export_api'
    # Iterate over each api_name
    for api_name in api_names:
        # Iterate over each filename in the directory
        for filename in os.listdir(directory):
            if api_name == filename:
                if filename.endswith('.yaml') or filename.endswith('.yml'):
                    file_path = os.path.join(directory, filename)
                    with open(file_path, 'r') as file:
                        api_spec = yaml.safe_load(file)
                        api_specs.append((api_name, api_spec))
    extract_data = extract_api_info_from_yaml_with_openai(api_specs,search_text)
    extract_data = extract_data.strip()
    # Remove the triple backticks and `json` label
    cleaned_string = extract_data.strip('```json\n')

    # Parse the string as JSON
    json_response = json.loads(cleaned_string)

    # Output the JSON response
    #print(json.dumps(json_response, indent=2))
    return json_response


def perform_ai_search(user_input):
    try:      
        # print('enterred')      
        # userinput=f"""Improve on the user input to ensure coverage of related data with minimum words.
        # user input : {user_input}"""
        # response = client.chat.completions.create(
        # model=deployment_name,
        # messages=[
        #     {"role": "user", "content": userinput}
        #     ]
        # )
        search_fields = ["summary"]  # Fields to search within
        select_options = {"top": 5}  # Limit the number of results to top 5
        #search_text = "list of Global Admins by account number and fetch account details for each listed Global Admin as well"  # Search query text
        # search_text=response.choices[0].message.content
        #print(search_text)
        response = search_client.search(search_text=user_input, search_fields=search_fields, **select_options)
        names = [result.get("name") for result in response if result.get("name")]
        #print(blob_names)
        return names, response
    except Exception as e:
        st.error(f"Failed to search documents: {str(e)}")
        return {'message': 'Failed to serve OpenAPI spec'}, 500
