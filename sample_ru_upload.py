import pandas as pd
import datetime 
fileName = 'C:\\Users\\DerekSasthav\\OneDrive - Generate Capital\\Desktop\\STEM LCR Model 09.15.2021 FPA Edit (V2)_r7.xlsx'
tabName = 'RU'

start = datetime.datetime.now()
df = pd.read_excel(fileName, tabName, header=5  )
end = datetime.datetime.now()
print(df.head())
print(end-start)