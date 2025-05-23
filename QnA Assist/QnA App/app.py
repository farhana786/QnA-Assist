import requests
import json
import pandas as pd
import streamlit as st

st.title("QnA Assist API")

url = 'https://XXXX/prompt_query'

user_prompt = st.chat_input()

if user_prompt:
    input = {'prompt' : user_prompt}
    response = requests.post(url, json = input, verify=False)
    response_json = response.json()

    if 'data' in response_json:
        # Extract the relevant data; it's a list of dictionaries
        answer_data = response_json['data']

        # Convert the extracted data to a DataFrame
        df = pd.DataFrame(answer_data)

        # Display the DataFrame in a tabular format
        st.dataframe(df)

    else:
        # Handle cases where the expected data is not present in the response
        st.error("The expected data is not present in the response.")
        st.json(response_json)  # Optional: Display the raw JSON for debugging
