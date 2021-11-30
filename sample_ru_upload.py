import pandas as pd
import datetime 

# Define function for column checking:
def column_check(x):
    if 'unnamed' in str(x).lower():
        return False
    if str(x).lower() in ['account', 'level', 'split label']:
        return True 
    if isinstance(x, datetime.datetime):
        return True
    return True

fileName = 'C:\\Users\\DerekSasthav\\OneDrive - Generate Capital\\Desktop\\test_RU.xlsx'
#fileName = 'test_RU.xlsx'

tabName = 'RU'

start = datetime.datetime.now()
df = pd.read_excel(fileName, tabName, header=4,engine = 'openpyxl', usecols = column_check)
end = datetime.datetime.now()
#print([isinstance(x, datetime.datetime) for x in list(df.columns)])
#print(df.head(20))
print(end-start)


# remove bad records
firstNullIndex = min(df["Account"].loc[df["Account"].isnull()].index.to_list())
dfClean = df.loc[:firstNullIndex-1,]
print(dfClean.head(10))


