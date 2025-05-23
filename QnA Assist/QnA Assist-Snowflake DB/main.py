import os
import re
import streamlit as st
from openai import AzureOpenAI
from prompts import get_system_prompt
from snowflake_utils import get_snowflake_connection
from dotenv import load_dotenv
import json
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex, SimpleField, SearchableField, SearchFieldDataType
)

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

# Define search parameters
search_fields = ["summary"]  # Fields to search within
select_options = {"top": 5}  # Limit the number of results to top 5

# Prompt for user input and save
user_prompt = st.chat_input()
if user_prompt:
    res = search_client.search(search_text=user_prompt, search_fields=search_fields, **select_options)
    summary = [result.get("summary") for result in res if result.get("summary")]
    st.session_state.messages.append({"role": "user", "content": user_prompt})
    st.session_state.messages.append({"role": "system", "content": " ".join(summary)})

# Display the existing chat messages
for message in st.session_state.messages:
    if message["role"] == "system":
        continue
    with st.chat_message(message["role"]):
        if "results" in message:
            st.dataframe(message["results"])
        else:
            st.markdown(message["content"])

# If last message is not from assistant, we need to generate a new response
if st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        response = ""
        resp_container = st.empty()
        for delta in client.chat.completions.create(
            model=deployment_name,
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
            stream=True,
        ):
            if delta.choices:
                response += (delta.choices[0].delta.content or "")
        message = {"role": "assistant", "content": response}
        
        # Parse the response for a SQL query and execute if available
        sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)
        
        if sql_match:
            sql = sql_match.group(1)
            conn = get_snowflake_connection()
            cursor = conn.cursor()
            try:
                # Run the SQL query and fetch the results
                cursor.execute(sql)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(results, columns=columns)
                # Display the results
                st.dataframe(df)
                # Update the message to only show that results are displayed
                message = {"role": "assistant", "content": "Here are the results:", "results": df}
            except Exception as e:
                message = {"role": "assistant", "content": f"An error occurred: {e}"}
        st.session_state.messages.append(message)
