import pandas as pd
import datetime as dt
import numpy as np

#מעבר מאלפי איש לקנה מידה של אנשים
population_data= pd.read_csv("datasets/population_data.csv", index_col=0)
population_data_new = population_data
population_data_new['population'] = population_data['population']*1000
population_data_new.to_csv("population_data_new.csv")
population_data_new


def date_to_datetime(data):
    data['DATE']= pd.to_datetime(data.DATE, format='%d/%m/%Y')
    data['DATE'] = data['DATE'].dt.date
    return data

unemployment_data= pd.read_csv("datasets/unemployment_data.csv", index_col=0)
unemployment_data = date_to_datetime(unemployment_data)
bpunemp= pd.read_csv("datasets/bpunemp.csv")

civilian_data= pd.read_csv("datasets/civilian_data.csv", index_col=0)
bpclf= pd.read_csv("datasets/bpclf.csv")

personal_income_data= pd.read_csv("datasets/personal income_data.csv", index_col=0)
lvRPI= pd.read_csv("datasets/lvRPI.csv")
bpRPI= pd.read_csv("datasets/bpRPI.csv")

def set_data(data):
    dates = data['DATE']
    dates = [dates[i].replace("-", "/") for i in range(0,len(dates))]
    dates = [f'{i.split("/")[2]}/{i.split("/")[1]}/{i.split("/")[0]}' for i in dates]
    data['DATE'] = dates
    data_df = date_to_datetime(data)
    return data_df




bpunemp = set_data(bpunemp)
bpunemp.rename(columns={ bpunemp.columns[1]: "Unemployment_Rate" }, inplace = True)
bpunemp['MSA']='Bridgeport-Stamford-Norwalk'
bpunemp['State']='CT'
bpunemp['1_Month_change']=bpunemp['Unemployment_Rate'].pct_change()
bpunemp['1_Year_change']=bpunemp['Unemployment_Rate'].pct_change(periods=12)

unemployment_data_new = pd.concat([unemployment_data,bpunemp])
unemployment_data_new = unemployment_data_new.reset_index()
unemployment_data_new = unemployment_data_new.drop(columns=['index'])
unemployment_data_new.to_csv("unemployment_data_new.csv")





bpclf = set_data(bpclf)
bpclf.rename(columns={ bpclf.columns[1]: "civilian" }, inplace = True)
bpclf['MSA']='Bridgeport-Stamford-Norwalk'
bpclf['State']='CT'
bpclf['1_Month_change']=bpclf['civilian'].pct_change()
bpclf['1_Year_change']=bpclf['civilian'].pct_change(periods=12)

civilian_data_new = pd.concat([civilian_data,bpclf])
civilian_data_new = civilian_data_new.reset_index()
civilian_data_new = civilian_data_new.drop(columns=['index'])
civilian_data_new.to_csv("civilian_data_new.csv")






bpRPI = set_data(bpRPI)
bpRPI.rename(columns={ bpRPI.columns[1]: "personal income" }, inplace = True)
bpRPI['MSA']='Bridgeport-Stamford-Norwalk'
bpRPI['State']='CT'
bpRPI['1_Year_change']=bpRPI['personal income'].pct_change(periods=12)

lvRPI = set_data(lvRPI)
lvRPI.rename(columns={ lvRPI.columns[1]: "personal income" }, inplace = True)
lvRPI['MSA']='Las Vegas-Henderson-Paradise'
lvRPI['State']='NV'
lvRPI['1_Year_change']=lvRPI['personal income'].pct_change(periods=12)

personal_income_data_new = pd.concat([personal_income_data,lvRPI,bpRPI])
personal_income_data_new = personal_income_data_new.reset_index()
personal_income_data_new = personal_income_data_new.drop(columns=['index'])
personal_income_data_new.to_csv("personal_income_data_new.csv")
