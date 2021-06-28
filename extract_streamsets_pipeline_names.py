import json 
import pandas as pd 
import os

# Set file path properties
base_file_path = 'C:\\Users\\DerekSasthav\\OneDrive - Generate Capital\\Desktop\\streamsets'
titles = []

# loop through each file in directory
for filename in os.listdir(base_file_path)[:2]:
    if filename.endswith(".json"):
        print(filename)
        full_filename = base_file_path + '\\' + filename
        with open(full_filename) as f:
            data = json.load(f) 
            title = data["pipelineConfig"]["title"]
            pipelineID = data["pipelineConfig"]["pipelineId"]
            payload = {'title':title, 'pipelineID':pipelineID}
            print(pipelineID, title)

print(titles)