import requests
import pandas as pd
from bs4 import BeautifulSoup
import fnmatch
import io
import re
from tqdm import tqdm
from random import randint
import time


###Subject Dictionaries
headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36"}
subjects = ['unemployment','gdp','personal income','civilian','population']

kw_dict={
    'unemployment':['Unemployment','Rate','in','(MSA)'],
    'gdp': ['Total','Real','Gross','Domestic','Product','for','(MSA)'],
    'personal income': ['Real','Per','Capita','Personal','Income','for','(MSA)'],
    'civilian': ['Civilian','Labor','Force','in','(MSA)'],
    'population': ['Resident','Population','in','(MSA)']
}

name_regex = {
    'unemployment': r'in (.*),',
    'gdp': r'for (.*),',
    'personal income': r'for (.*),',
    'civilian': r'in (.*),',
    'population': r'in (.*),'    
}

freq_dict = {
    'unemployment':'m',
    'gdp': 'y',
    'personal income':'y',
    'civilian':'m',
    'population':'y'
}

###Scraping Loop for All Subjects
for subject in subjects:
    final_df = pd.DataFrame() 
    i=0 #DF existence switch
    msa_dict = {}
    #Getting datasets' names
    for page_num in tqdm(range(1,100), position = 0, leave = True):
        subject_urls={
    'unemployement':f'https://fred.stlouisfed.org/searchresults/?st=MSA&t={subject}%3Bnsa&ob=sr&od=desc&types=gen;seas&pageID={page_num}',
    'gdp': f'https://fred.stlouisfed.org/searchresults/?st=MSA&t={subject}%3Breal&ob=sr&od=desc&types=gen&pageID={page_num}',
    'personal income': f'https://fred.stlouisfed.org/searchresults/?st=MSA&t={subject}%3Breal&ob=sr&od=desc&types=gen&pageID={page_num}',
    'civilian': f'https://fred.stlouisfed.org/searchresults/?st=MSA&t={subject}%3Bmonthly%3Bnsa&ob=sr&od=desc&types=gen;seas&pageID={page_num}',
    'population': f'https://fred.stlouisfed.org/searchresults/?st={subject}&t=msa&ob=sr&od=desc&types=gen;geot&pageID={page_num}',
}
        subject_url = subject_urls[subject]
        response = requests.get(subject_url,headers=headers)
        soup = BeautifulSoup(response.content.decode(),features='lxml')
        urls = soup.findAll("a",class_='series-title search-series-title-gtm',href=True)
        if urls:
            f_urls = [x for x in urls if all(substr in x.text for substr in kw_dict[subject])]
            for url in f_urls:
                #Filtering urls by keywords from kw_dict
                msa_dict[re.search(name_regex[subject], url.text).group(1)] = [
                    re.search(r',(.* )', url.text).group(1).strip(),
                    re.search(r'[^/]*$', url['href']).group(0)] 
            time.sleep(randint(2,5))
        else:
            break
    #Downloading csv files to final dataframe
    for MSA in tqdm(list(msa_dict.keys()), position = 0, leave = True):
        #Picking url type by subject
        url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1169&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id={msa_dict[MSA][1]}&scale=left&cosd=1990-01-01&coed=2021-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2021-04-01&revision_date=2021-04-01&nd=1990-01-01'
        #Looping until download is successful
        while True:
            try:
                r = requests.post(url,headers=headers)
                data = r.content.decode('utf8')
                df = pd.read_csv(io.StringIO(data))
                df = df.rename(columns={f"{msa_dict[MSA][1]}": f"{subject}"})
                df['MSA'] = MSA
                df['State'] = msa_dict[MSA][0]
                #creating columns according to data frequency- Yearly, Monthly or Quarterly
                if freq_dict[subject]=='m':
                    df['1_Month_change'] = pd.DataFrame(df[f"{subject}"]).pct_change(periods=1, limit=None, freq=None)
                    df['1_Year_change'] = pd.DataFrame(df[f"{subject}"]).pct_change(periods=12, limit=None, freq=None)
                elif freq_dict[subject]=='y':
                    df['1_Year_change'] = pd.DataFrame(df[f"{subject}"]).pct_change(periods=1, limit=None, freq=None)
                elif freq_dict[subject]=='q':
                    df['1_Quarter_change'] = pd.DataFrame(df[f"{subject}"]).pct_change(periods=1, limit=None, freq=None)
                    df['1_Year_change'] = pd.DataFrame(df[f"{subject}"]).pct_change(periods=4, limit=None, freq=None)   
                break
            except Exception: 
                time.sleep(randint(5,10))
                pass
        if i==1:
            final_df = pd.concat([final_df, df], ignore_index=True)
        else:
            final_df = df
            i=1
        time.sleep(randint(5,10))
    #Saving final df
    final_df.to_csv(f'{subject}_data.csv')