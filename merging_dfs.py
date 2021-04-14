import pandas as pd
import numpy as np
from tqdm import tqdm
#Uploading Dataframes
une_df = pd.read_csv('unemployment_data_new.csv')
une_df = une_df.loc[:, ~une_df.columns.str.contains('^Unnamed')]
pop_df = pd.read_csv('population_data_new.csv')  #תיקון שם קובץ-דניאל
pop_df = pop_df.loc[:, ~pop_df.columns.str.contains('^Unnamed')]
gdp_df = pd.read_csv('gdp_data.csv')
gdp_df = gdp_df.loc[:, ~gdp_df.columns.str.contains('^Unnamed')]
pinc_df = pd.read_csv('personal_income_data_new.csv')   #תיקון שם קובץ-דניאל
pinc_df = pinc_df.loc[:, ~pinc_df.columns.str.contains('^Unnamed')]
civ_df = pd.read_csv('civilian_data_new.csv')  #תיקון שם קובץ-דניאל
civ_df = civ_df.loc[:, ~civ_df.columns.str.contains('^Unnamed')]
HPI_df = pd.read_csv('HPI_mean_data.csv')
HPI_df = HPI_df.loc[:, ~HPI_df.columns.str.contains('^Unnamed')]

#Merging Dataframes
dfs = [df.set_index(['DATE', 'MSA','State']) for df in [une_df, civ_df, pop_df,pinc_df,gdp_df]]
merge_df = pd.concat(dfs, axis=1).reset_index()

#Creating civilian-population ratio column
# דניאל - הוכנס בקוד הקודם לתוך הדאטא של האוכ׳
#merge_df['civil_pop_ratio'] = merge_df.apply(lambda x: x['civilian']/(x['population']*1000),axis=1)

#Fixing State names bug
merge_df['State'] = [x.split(" (MSA)")[0] for x in merge_df['State']]

#Creating short date column for merging with HPI
merge_df['Short_Date'] = [x[:7] for x in merge_df['DATE']]
merge_df = merge_df.loc[:, ~merge_df.columns.str.contains('DATE')]
HPI_df['Short_Date'] = [x[:7] for x in HPI_df['DATE']]
HPI_df = HPI_df.loc[:, ~HPI_df.columns.str.contains('DATE')]

#Matching MSA and State names between dataframes
msa_dict={}
for index,row in merge_df.iterrows():
    if row['MSA'] in list(msa_dict.keys()):
        continue
    msa_dict[row['MSA']]= row['State']

for key in tqdm(list(msa_dict.keys())):
    for index,row in HPI_df.iterrows():
        if all(element in key.split("-") for element in row['MSA'].split("-")):
            if all(element in msa_dict[key].split("-") for element in row['State'].split("-")):
                HPI_df.loc[index,'MSA'] = key
                HPI_df.loc[index,'State'] = msa_dict[key]

# Merging HPI_df with merged_df
#dfs2 = [df.set_index(['Short_Date', 'MSA','State']) for df in [merge_df,HPI_df]]
#final_df = pd.concat(dfs2, axis=1).reset_index()
#final_df.to_csv('final_df.csv')

hpis=[]
hpis1=[]
hpis2=[]
hpis3=[]

for row in tqdm(merge_df.index):
    state = merge_df['State'][row]
    msa = merge_df['MSA'][row]
    date = merge_df['Short_Date'][row]
    date1 = f'{int(merge_df["Short_Date"][row].split("-")[0])+1}-{merge_df["Short_Date"][row].split("-")[1]}'
    date2 = f'{int(merge_df["Short_Date"][row].split("-")[0])+2}-{merge_df["Short_Date"][row].split("-")[1]}'
    date3 = f'{int(merge_df["Short_Date"][row].split("-")[0])+3}-{merge_df["Short_Date"][row].split("-")[1]}'
    
    #price today
    df = HPI_df.loc[(HPI_df['State'] == state) & (HPI_df['MSA'] == msa) & (HPI_df['Short_Date'] == date)]
    if len(df) > 0 :
        df = df[np.isnan(df['HPI'])==False]    #example: Huntington-Ashland
        if len(df)== 0:
            hpis.append(np.nan)
        elif len(df)== 1:
            hpis.append(df['HPI'].item())
        else:
            hpis.append(df['HPI'].mean())
        
    else:
        hpis.append(np.nan)
        
    #price in 1 year
    df1 = HPI_df.loc[(HPI_df['State'] == state) & (HPI_df['MSA'] == msa) & (HPI_df['Short_Date'] == date1)]
    if len(df1) > 0 :
        df1 = df1[np.isnan(df1['HPI'])==False]    #example: Huntington-Ashland
        if len(df1)== 0:
            hpis1.append(np.nan)
        elif len(df1)== 1:
            hpis1.append(df1['HPI'].item())
        else:
            hpis1.append(df1['HPI'].mean())
        
    else:
        hpis1.append(np.nan)
        
    #price in 2 years     
    df2 = HPI_df.loc[(HPI_df['State'] == state) & (HPI_df['MSA'] == msa) & (HPI_df['Short_Date'] == date2)]
    if len(df2) > 0 :
        df2 = df2[np.isnan(df2['HPI'])==False]    #example: Huntington-Ashland
        if len(df2)== 0:
            hpis2.append(np.nan)
        elif len(df2)== 1:
            hpis2.append(df2['HPI'].item())
        else:
            hpis2.append(df2['HPI'].mean())
        
    else:
        hpis2.append(np.nan)
    
    #price in 3 years    
    df3 = HPI_df.loc[(HPI_df['State'] == state) & (HPI_df['MSA'] == msa) & (HPI_df['Short_Date'] == date3)]
    if len(df3) > 0 :
        df3 = df3[np.isnan(df3['HPI'])==False]    #example: Huntington-Ashland
        if len(df3)== 0:
            hpis3.append(np.nan)
        elif len(df3)== 1:
            hpis3.append(df3['HPI'].item())
        else:
            hpis3.append(df3['HPI'].mean())
        
    else:
        hpis3.append(np.nan)
        
merge_df['HPI'] = hpis        
merge_df['HPI_1_year'] = hpis1
merge_df['HPI_2_year'] = hpis2 
merge_df['HPI_3_year'] = hpis3 
merge_df.to_csv('final_with_future_hpis.csv')
