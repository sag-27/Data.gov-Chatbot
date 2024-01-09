#!/usr/bin/env python
# coding: utf-8

# In[42]:


'''
Data Downloader Module:
 Develop a module to connect to the Data.gov API and download datasets in CSV format.
 Allow users to specify search parameters (e.g., keywords, data formats) for targeted downloads.
 Implement error handling and logging to capture download activities.
'''


# In[43]:


'''
Enhanced Logging:
 Implement a robust logging system using Python's logging module to capture events, errors, and activities within the application.
 Log critical information such as API requests, download status, and chatbot interactions.
'''


# In[44]:


import requests
import logging
import pandas as pd
import os


# In[45]:


class CsvApiDownloader:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.base_url = 'https://api.data.gov.in/resource/'
        self.headers = {"api-key": self.api_key} if self.api_key else {}
        
        # configuration
        self.logger = logging.getLogger("CsvApiDownloader")
        self.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler('download_log.txt')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def download_csv(self, api_endpoint, output_folder="datasets"):
        try:
            os.makedirs(output_folder, exist_ok=True)

            # format must be csv, json or xml
            params = {"api-key": self.api_key, "format": "csv"}
            response = requests.get(f"{self.base_url}{api_endpoint}", params=params, headers=self.headers)
            response.raise_for_status()

            file_name = f"{api_endpoint}.csv"
            file_path = os.path.join(output_folder, file_name)

            with open(file_path, "wb") as csv_file:
                csv_file.write(response.content)

            # log download successful
            self.logger.info(f"Download successful for API endpoint: {api_endpoint}")

            # contents of the downloaded CSV file
            print(f"Contents of the downloaded CSV file ({file_path}):")
            with open(file_path, "r", encoding="utf-8") as print_file:
                print(print_file.read())

            self.view_dataset(file_path)

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error in API request: {e}")

    def view_dataset(self, file_path):
        try:
            df = pd.read_csv(file_path)
            print("\nDataFrame:")
            print(df)
            
            # additional errors incase encountered any
        except pd.errors.EmptyDataError:
            print("\nDataFrame is empty.")
        except pd.errors.ParserError:
            print("\nError parsing CSV file. Check the file format.")

if __name__ == "__main__":
    # necessary inputs 
    api_key = input("Enter your API key: ")
    downloader = CsvApiDownloader(api_key)
    api_endpoint = input("Enter the API endpoint: ")
    downloader.download_csv(api_endpoint, output_folder="datasets")


# In[46]:


api_key = '579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b'

api_endpoint = '352b3616-9d3d-42e5-80af-7d21a2a53fab'


# In[47]:


'''
FastAPI Integration:
 Utilize FastAPI to create a RESTful API that serves as the backend for the project.
 Create API endpoints for downloading datasets and retrieving information about downloaded datasets.
'''


# In[48]:


get_ipython().system('pip install fastapi==0.68.0')


# In[49]:


get_ipython().system('pip install starlette==0.14.2')


# In[50]:


get_ipython().system('pip install nest-asyncio')


# In[51]:


import subprocess
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import nest_asyncio


# In[52]:


nest_asyncio.apply()


# In[53]:


app = FastAPI()


# In[54]:


class CsvApiDownloader:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.base_url = 'https://api.data.gov.in/resource/'
        self.headers = {"api-key": self.api_key} if self.api_key else {}

        # configure
        self.logger = logging.getLogger("CsvApiDownloader")
        self.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler('download_log.txt')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def download_csv(self, api_endpoint, output_folder="datasets"):
        try:
            os.makedirs(output_folder, exist_ok=True)
            params = {"api-key": self.api_key, "format": "csv"}
            response = requests.get(f"{self.base_url}{api_endpoint}", params=params, headers=self.headers)
            response.raise_for_status()

            file_name = f"{api_endpoint}.csv"
            file_path = os.path.join(output_folder, file_name)

            with open(file_path, "wb") as csv_file:
                csv_file.write(response.content)

            self.logger.info(f"Download successful for API endpoint: {api_endpoint}")

            return file_path

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error in API request: {e}")
            raise HTTPException(status_code=500, detail=f"Error in API request: {e}")


# In[55]:


downloader = CsvApiDownloader()


# In[56]:


class DownloadRequest(BaseModel):
    api_key: str
    api_endpoint: str
    output_folder: str = "datasets"

@app.post("/download-csv")
def download_csv(request: DownloadRequest):
    try:
        downloader.api_key = request.api_key  # api dynamic key setup
        file_path = downloader.download_csv(request.api_endpoint, output_folder=request.output_folder)
        return {"file_path": file_path}
    except HTTPException as e:
        return {"error": str(e)}


# In[57]:


uvicorn_process = subprocess.Popen(["uvicorn", "main:app"])


# In[58]:


# uvicorn_process.terminate()


# In[59]:


'''
OpenAI Chatbot:
 Sign up for a free OpenAI API key to access the GPT-based language model.
 Implement a chatbot module that uses the OpenAI API to respond to user queries related to the downloaded datasets.
'''


# In[60]:


get_ipython().system('pip install openai==0.28')


# In[61]:


import openai


# In[62]:


import subprocess


# In[63]:


class Chatbot:
    def __init__(self, openai_api_key):
        self.openai_api_key = openai_api_key

    def generate_response(self, user_query):
        openai.api_key = self.openai_api_key

        # any prompt
        prompt = f"Given the dataset: '{user_query}', provide relevant information."

        # generate response sing openai
        response = openai.Completion.create(
            engine="gpt-3.5-turbo-instruct",  # problem with engine: text-davinci-003,text-davinci-002 ..
            prompt=prompt,
            max_tokens=150 
        )

        return response.choices[0].text.strip()


# In[64]:


chatbot = Chatbot(openai_api_key)


# In[65]:


# example query
user_query = "What are the key insights from the dataset?"
chatbot_response = chatbot.generate_response(user_query)

print(f"Chatbot Response: {chatbot_response}")

