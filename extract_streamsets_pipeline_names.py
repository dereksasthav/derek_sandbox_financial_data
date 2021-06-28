import json 
import pandas as pd 
import os

# Set file path properties
base_file_path = 'C:\Users\DerekSasthav\OneDrive - Generate Capital\Desktop\streamsets'

# loop through each file in directory
for filename in os.lisdir(base_file_path):
    if filename.endswith(".json"):
        print(filename)
