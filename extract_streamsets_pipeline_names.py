import json 
import pandas as pd 
import os

# Set file path properties
#base_file_path = 'C:\\Users\\DerekSasthav\\OneDrive - Generate Capital\\Desktop\\streamsets'
base_file_path = '/datadrive/Streamsets/lib/sdc/pipelines'
titles = []

# loop through each file in directory
for filename in os.listdir(base_file_path):
    try:
        print(filename)
        full_filename = base_file_path + '/' + filename + '/' 'info.json'
        with open(full_filename) as f:
            data = json.load(f) 
            title = data["title"]
            pipelineID = data["pipelineId"]
            tags = "|".join(data["metadata"]["labels"])
            payload = {'title': title, 'pipelineID': pipelineID, 'tags': tags}
            titles.append(payload)
            print(pipelineID, title, tags)
    except Exception as e:
        print("Error occurred for %s , %s" % (full_filename, str(e)))

print(titles)
df = pd.DataFrame(titles)
print(df.head())
df.to_csv('StreamSets_Pipeline_Titles.csv', index=False,sep=",")
print("CSV Output Complete!")