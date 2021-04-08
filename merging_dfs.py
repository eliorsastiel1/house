import pandas as pd
import numpy as np
from tqdm import tqdm
#Uploading Dataframes
une_df = pd.read_csv('unemployment_data_new.csv')
une_df = une_df.loc[:, ~une_df.columns.str.contains('^Unnamed')]
pop_df = pd.read_csv('population_data.csv')
pop_df = pop_df.loc[:, ~pop_df.columns.str.contains('^Unnamed')]
gdp_df = pd.read_csv('gdp_data.csv')
gdp_df = gdp_df.loc[:, ~gdp_df.columns.str.contains('^Unnamed')]
pinc_df = pd.read_csv('personal income_data.csv')
pinc_df = pinc_df.loc[:, ~pinc_df.columns.str.contains('^Unnamed')]
civ_df = pd.read_csv('civilian_data.csv')
civ_df = civ_df.loc[:, ~civ_df.columns.str.contains('^Unnamed')]
HPI_df = pd.read_csv('HPI_mean_data.csv')
HPI_df = HPI_df.loc[:, ~HPI_df.columns.str.contains('^Unnamed')]

#Merging Dataframes
dfs = [df.set_index(['DATE', 'MSA','State']) for df in [une_df, civ_df, pop_df,pinc_df,gdp_df]]
merge_df = pd.concat(dfs, axis=1).reset_index()

#Creating civilian-population ratio column
#הוכנס בקוד הקודם לתוך הדאטא של האוכ׳
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
dfs2 = [df.set_index(['Short_Date', 'MSA','State']) for df in [merge_df,HPI_df]]
final_df = pd.concat(dfs2, axis=1).reset_index()

final_df.to_csv('final_df.csv')

