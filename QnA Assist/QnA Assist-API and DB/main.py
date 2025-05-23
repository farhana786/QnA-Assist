import os
import re
import streamlit as st
import pandas as pd
from openai import AzureOpenAI
from prompts import get_system_prompt, get_api_prompt, get_tables_prompt, perform_ai_search, extract_and_index_api_spec_from_blobs
from snowflake_utils import get_snowflake_connection
from dotenv import load_dotenv
import json
import requests
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

st.title("Data QnA Assist")

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

# Initialize the chat messages history
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "system", "content": get_system_prompt()}]
    st.session_state.messages.append({"role": "assistant", "content": "Hello! How may I assist you today?"})

# Display the existing chat messages
for message in st.session_state.messages:
    if message["role"] == "system":
        continue
    with st.chat_message(message["role"]):
        if "results" in message:
            st.dataframe(message["results"])
        else:
            st.markdown(message["content"])

# Define search parameters
search_fields = ["name"]  # Fields to search within
select_options = {"top": 5}  # Limit the number of results to top 10

def fetch_api_data(api_context):
    response = ""
    resp_container = st.empty()
    for delta in client.chat.completions.create(
        model=deployment_name,
        messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
        stream=True,
    ):
        
        if delta.choices:
            response += (delta.choices[0].delta.content or "")
            #resp_container.markdown(response)

    message = {"role": "assistant", "content": response}

    token_url = 'https://servnow.oktapreview.com/oauth2/aus1e3ov8pyM8n9OV0h8/v1/token'

    token_headers = {
        'Authorization': 'Basic MG9hMXgxZWhrcjh1ZEVIWEIwaDg6TXJqa1BwSkE5R2RKTHBTSHZ1dktBcWEyeFJDdUp3TzZXM3RJVy1uZHZJWnNYUkxtaDVWZnJtcWZNamNfbWRsRg==',
        'Content-Type': 'application/x-www-form-urlencoded','User-Agent': 'insomnia/8.4.3'
                    }

    token_data = {
        'grant_type': 'client_credentials',
        'scope': 'dtservices.ems.entitlement.admin'
                 }       

    token_response = requests.post(token_url, headers=token_headers, data=token_data)

    if token_response.status_code == 200:
        token = token_response.json().get('access_token')
    else:
        st.write(f"Failed to generate token: {token_response.status_code}")
        #st.write(token_response.json())
        exit()

    api_headers = {
                    'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json'
                }
    # Parse the response for a REST API HTTP URL and execute if available
    url_match = re.search(r"```GET\n(.*)\n```", response, re.DOTALL)
    #st.write("URL_Match: ", url_match) 

    if url_match:
        url = url_match.group(1)
        #st.write(url)
        try:
            api_response = requests.get(url, headers=api_headers)
            message["results"] = api_response.json()
            st.write(message["results"])
        except Exception as e:
            st.write(f"An error occurred: {e}")
    st.session_state.messages.append(message)

def fetch_db_data(table_context):
    response = ""
    resp_container = st.empty()
    for delta in client.chat.completions.create(
        model=deployment_name,
        messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
        stream=True,
    ):
        if delta.choices:
            response += (delta.choices[0].delta.content or "")
            #resp_container.markdown(response)

    message = {"role": "assistant", "content": response}

    sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)

    if sql_match:
        sql = sql_match.group(1)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            st.dataframe(df)
            message = {"role": "assistant", "content": "Here are the results:", "results": df}
        except Exception as e:
            message = {"role": "assistant", "content": f"An error occurred: {e}"}
    else:
        message = {"role": "assistant", "content": "No valid SQL query found in the response."}

    st.session_state.messages.append(message)

# Prompt for user input and save
user_prompt = st.chat_input()
if user_prompt:
    # name_list, search_text = perform_ai_search(user_prompt)
    # st.write(search_text)
    res = search_client.search(search_text=user_prompt, search_fields=search_fields, **select_options)
    name_list = [result.get("name") for result in res if result.get("name")]
    st.write(name_list)
    st.session_state.messages.append({"role": "user", "content": user_prompt})
    yaml_count = sum(1 for file in name_list if file.endswith(('.yaml', '.yml')))
    st.write(yaml_count)
    yaml_files = [name for name in name_list if name.endswith('.yaml') or name.endswith('.yml')]
    file_names = [file for index, file in enumerate(yaml_files)]
    #st.write(file_names)
    table_names = [name for name in name_list if not (name.endswith('.yaml') or name.endswith('.yml'))]
        
    if yaml_count >= 3:
        # json_response = extract_and_index_api_spec_from_blobs(file_names, search_text)
        # st.write(json_response)
        api_summary = get_api_prompt(file_names)
        #st.write(api_summary)
        st.session_state.messages.append({"role": "system", "content": api_summary})
        fetch_api_data(api_summary)
    else:
        tables_summary = get_tables_prompt(table_names)
        #st.write(tables_summary)
        st.session_state.messages.append({"role": "system", "content": tables_summary})
        fetch_db_data(tables_summary)
