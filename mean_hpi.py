import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np

HPI_low_tier = pd.read_csv("datasets/Metro_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_mon.csv")
HPI_low_tier.insert(3,"Region",[row.split(",")[0] for row in HPI_low_tier['RegionName']])

HPI_medium_tier = pd.read_csv("datasets/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon.csv")
HPI_medium_tier.insert(3,"Region",[row.split(",")[0] for row in HPI_medium_tier['RegionName']])

HPI_high_tier = pd.read_csv("datasets/Metro_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_mon.csv")
HPI_high_tier.insert(3,"Region",[row.split(",")[0] for row in HPI_high_tier['RegionName']])


rows = [list(HPI_low_tier.columns)]
for Region_id in HPI_low_tier['RegionID']:
    low = HPI_low_tier[HPI_low_tier['RegionID']==Region_id]
    medium = HPI_medium_tier[HPI_medium_tier['RegionID']==Region_id]
    high = HPI_high_tier[HPI_high_tier['RegionID']==Region_id]
    row = []
    for val in low.columns[:6]:
        row.append(low[val].item())
    for col in low.columns[6:]:
        mean = np.mean([low[col],medium[col],high[col]])
        row.append(mean)   
    rows.append(row)     
    
mean_hpi = pd.DataFrame(rows)
new_header = mean_hpi.iloc[0] 
mean_hpi = mean_hpi[1:] 
mean_hpi.columns = new_header     

mean_hpi.to_csv("Metro_zhvi_uc_sfrcondo_tier_mean_sm_sa_mon.csv")


def change_rows(data,name):
    data = data.drop(columns=['RegionID', 'SizeRank','RegionName','RegionType'])
    data = data.rename(columns={'Region': 'MSA', "StateName":"State"})

    vals = [["DATE","HPI","MSA","State"]]
    for row in data.index[1:]:
        for col in data.columns[2:]:
            vals.append([col,data[col][row],data["MSA"][row],data["State"][row]])
    
    hpi = pd.DataFrame(vals)
    new_header = hpi.iloc[0] 
    hpi = hpi[1:] 
    hpi.columns = new_header
    
    hpi = hpi.reset_index()
    hpi = hpi.drop(columns=['index'])
    
    hpi['1_Month_change'] = hpi['HPI'].pct_change()
    hpi['1_Year_change'] = hpi['HPI'].pct_change(periods=12)

    hpi.to_csv(f"{name}_data.csv")
    
    return hpi


change_rows(HPI_high_tier,"HPI_high_tier")
change_rows(HPI_medium_tier,"HPI_medium_tier")
change_rows(HPI_low_tier,"HPI_low_tier")
change_rows(mean_hpi,"HPI_mean")

