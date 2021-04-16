import pandas as pd
import numpy as np
df = pd.read_csv('final_with_future_hpis.csv')
cols = ['MSA','State','Short_Date']
df = df.fillna(-1)
df['combined'] = df[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
df2 = df.groupby(by=["combined","MSA","State","Short_Date"],dropna=False).sum()
df2.reset_index(inplace=True)
df2 = df2.drop(['combined', 'Unnamed: 0'], axis=1)
df2 = df2.replace([-1,-2],np.nan)
df2.to_csv('data_to_model.csv')