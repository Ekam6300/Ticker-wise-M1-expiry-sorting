import re
import pandas as pd
import os
from datetime import datetime as dt
import zipfile
import glob
import shutil

years = []

for file in os.listdir(os.getcwd()):
    if file.endswith(".zip"):
        years.append(file)


yy = []        

def yearExtractor(y):
    with zipfile.ZipFile(y,'r') as myZip:
        newy = y
        newy = newy.removesuffix('.zip')
        yy.append(newy)
        myZip.extractall(f"{newy}")   
        
def monthExtractor(m):
    dir_name = os.path.join(os.getcwd()+ f"/{m}")
    extension = ".zip"

    os.chdir(dir_name)

    for item in os.listdir(dir_name): 
        if item.endswith(extension):
            file_name = os.path.abspath(item)
            zip_ref = zipfile.ZipFile(file_name) 
            zip_ref.extractall(dir_name) 
            zip_ref.close()
            os.remove(file_name) 
    os.chdir("../")
        
x = list(map(yearExtractor,years))
x2 = list(map(monthExtractor,yy))




df_list = []

def stock(s):
    # ye=year[-2:]
    ye=18
    day = pd.read_csv(s)
    # day.drop_duplicates(subset="Ticker")
    # print(day)
    sub_list = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    day['mont']= 0
    day['num'] = 0 
    day = day[day["Ticker"].str.contains("NIFTY")==False] 
    day.reset_index(drop=True,inplace=True )
    for rows in range(len(day)):
        if "JAN" in day.loc[rows,"Ticker"]:
            if f"{str(ye)}JAN" in day.loc[rows,"Ticker"]:
                day.loc[rows,"mont"]="JAN"
                day.loc[rows,"num"] = 1
            else:
                day.loc[rows,"mont"]="JAN"
                day.loc[rows,"num"] = 13
        elif 'FEB' in day.loc[rows,"Ticker"]:
            if f"{str(ye)}FEB" in day.loc[rows,"Ticker"]:
                day.loc[rows,"mont"]="FEB"
                day.loc[rows,"num"] = 2
            else:
                day.loc[rows,"mont"]="FEB"
                day.loc[rows,'num'] = 14
        elif f'{str(ye)}MAR' in day.loc[rows,"Ticker"]:
                day.loc[rows,'mont']="MAR"
                day.loc[rows,'num'] = 3
        elif f'{int(ye)+1}MAR' in day.loc[rows,"Ticker"]:
                day.loc[rows,'mont']="MAR"
                day.loc[rows,'num'] = 15
        elif 'APR' in day.loc[rows,"Ticker"]:
            day.loc[rows,'mont']="APR"
            day.loc[rows,'num'] = 4
        elif 'MAY' in day.loc[rows,"Ticker"]:
            day.loc[rows,'mont']="MAY"
            day.loc[rows,'num'] = 5   
        elif 'JUN' in day.loc[rows,"Ticker"]:
            day.loc[rows,'mont']="JUN"
            day.loc[rows,'num'] = 6
        elif 'JUL' in day.loc[rows,"Ticker"]:
            day.loc[rows,'mont']="JUL"
            day.loc[rows,'num'] = 7
        elif 'AUG' in day.loc[rows,"Ticker"]:
            day.loc[rows,'mont']="AUG"
            day.loc[rows,'num'] = 8
        elif 'SEP' in day.loc[rows,"Ticker"]:
            day.loc[rows,'mont']="SEP"
            day.loc[rows,'num'] = 9
        elif 'OCT' in day.loc[rows,"Ticker"]:
            day.loc[rows,'mont']="OCT"
            day.loc[rows,'num'] = 10
        elif 'NOV' in day.loc[rows,"Ticker"]:
            day.loc[rows,'mont']="NOV"
            day.loc[rows,'num'] = 11
        elif 'DEC' in day.loc[rows,"Ticker"]:
            day.loc[rows,'mont']="DEC"
            day.loc[rows,'num'] = 12    

    minn= min(day["num"])
    day = day[day["num"]==minn]
    unames = list(day["Ticker"].unique())
    day.sort_values(by=['Date',"Time"],inplace=True,ignore_index=True)
    day.reset_index(drop=True,inplace=True)
    minna = day.loc[0,"mont"]
    print((minn,minna))
    sub_list = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']    
    for rows in range(len(day)):
        if day.loc[rows,'num']==minn: # condition not needed
            # print(minna,day.loc[rows,"Ticker"] )
            if minn > 12  :
                day.loc[rows,'Ticker'] = day.loc[rows,'Ticker'].replace(str(ye+1)+str(minna),'')
            day.loc[rows,'Ticker'] = day.loc[rows,'Ticker'].replace(str(ye)+str(minna),'')
            day.loc[rows,"Ticker"] = day.loc[rows,'Ticker'].replace(str(ye)+str(minna),'')
            day.loc[rows,'Ticker'] = day.loc[rows,'Ticker'].replace('CA.NFO','CE')
            day.loc[rows,'Ticker'] = day.loc[rows,'Ticker'].replace('PA.NFO','PE')
            day.loc[rows,"Ticker"] = day.loc[rows,'Ticker'].replace(".NFO","")
            day.loc[rows,'Ticker'] = day.loc[rows,'Ticker']+"M1"
    day.drop(["mont","num"], inplace=True, axis=1)
    print(day)
    df_list.append(day)


#################
#################
#################
#################
#################
#################

# For iterating the year and month to day to the stock function

yy = ["2018"]    
substring = "FUT"
path = os.listdir()
for year in yy:
    liM=[]
    ye = year[-2:]
    for month in os.listdir(os.path.join((os.getcwd()+"/"+year))):
        path = os.path.join((os.getcwd()+"/"+year+"/"+month))
        if (os.path.isdir(path)):
            liM.append(month)
    print(year,liM)
    for month in liM:
        print(month)
        path = os.getcwd()+f'/{year}/{month}'
        all_files = glob.glob(path + "/*.csv") 
        xxx = list(map(stock, all_files))
    
            

df = pd.concat(df_list ,axis=0)
df = df.sort_values(by=["Ticker","Date"],ignore_index=True)
print(df)
print(len(df))

# For seperating TickerM1 from final DataFrame

f = pd.DataFrame()

for i in range(len(df)):
    f=pd.concat([f,df.iloc[i].to_frame().T], ignore_index=True)
    if (i+1) == len(df):
        f.drop('Ticker',inplace=True, axis = 1)
        f.to_csv(f"Output/{df.loc[i,'Ticker']}.csv", index = False  )
        break

    if df.loc[i,"Ticker"]!=df.loc[i+1,"Ticker"]:
        f.drop('Ticker',inplace=True, axis = 1)
        f.to_csv(f"Output/{df.loc[i,'Ticker']}.csv", index = False  )
        f = pd.DataFrame()



