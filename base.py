#!/usr/bin/env python
# coding: utf-8

# # Pulling data 

# In[42]:


import requests 
import pandas as pd
from collections import defaultdict
from datetime import date 
import numpy as np
from scipy import stats 
import seaborn as sns 
import statsmodels.api as sm
import warnings
warnings.filterwarnings('ignore')
import re
import matplotlib.pyplot as plt
from datetime import timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta
from IPython.core.display import display, HTML, Markdown
import matplotlib.gridspec as gridspec
import dask 
# from dask.distributed import Client 
# from dask import delayed 
# from dask import compute
# import plotly
# from IPython.html.widgets import interactive
import qgrid
from ipywidgets.embed import  embed_minimal_html
import ipywidgets
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from IPython.display import display_html
import tabloo


# In[43]:


plt.rc("font",size = 15)
sns.set(style="dark")
sns.set(style="darkgrid")
sns.set(rc={'figure.figsize':(10,10)})
get_ipython().run_line_magic('matplotlib', 'inline')
pd.set_option('display.max_columns',500)
plt.rcParams['axes.labelsize'] = 14
plt.rcParams['xtick.labelsize'] = 12
plt.rcParams['ytick.labelsize'] = 12
plt.rcParams['text.color'] = 'k'
plt.style.use('fivethirtyeight')
pd.set_option('display.max_columns',None,'display.max_rows', None)
pd.options.display.float_format = '{:.2f}'.format 


# In[44]:


grid_opt = {
    # SlickGrid options
    'fullWidthRows': True,
    'syncColumnCellResize': True,
    'forceFitColumns': False,
    'defaultColumnWidth': 150,
    'rowHeight': 28,
    'enableColumnReorder': False,
    'enableTextSelectionOnCells': True,
    'editable': True,
    'autoEdit': False,
    'explicitInitialization': True,

    # Qgrid options
    'maxVisibleRows':20,
    'minVisibleRows': 8,
    'sortable': True,
    'filterable': True,
    'highlightSelectedCell': False,
    'highlightSelectedRow': True
}
col_opt = {
    # SlickGrid column options
    'defaultSortAsc': False,
    'maxWidth': None,
    'minWidth': 30,
    'resizable': True,
    'sortable': True,
    'toolTip': "",
    'width': None,

    # Qgrid column options
    'editable': True,
}


# # API FUNCTION

# In[45]:


def get_dfs(ticker_upper,insta = True):
  ''' Function that takes a ticker in upper case/ normal format and returns a dictionary of data frames for each trend of interest. the outputted columns include the name and titles of the query
  which include the source of data as well. The following are the dictionary's keys: 
  ['Gtrends1', 'Instagram', 'Revenue', 'Twitter',  'instagram_posts', 'Gtrends', 'AlexaUniqueVisits', 'AlexaTotalVisits', 'instagram_likes']'''
  
  #making api call 
  
  from datetime import timezone

  result = requests.get(url = f'https://app.sentieo.com/api/new_fetch_mosaic_summary_data/?ticker={ticker_upper.lower()}&first_call=true&selection=Revenue_corrScore&termtype=ticker&counter=1&_=1594838829331&loc=app&csrfmiddlewaretoken=czewr09umlKVCyB1eDNXKnlBosoDJWDY&apikey=0e17c30b4fd5f6a39dae2718375b2885f902f18fc661c36fa79ec55369ba2b5c')
  
  if result.status_code != 200: 
    print(f'Warning: API call needs adjustment, potentially invalid status code: {result.status_code}')
  else:
    #loading data 
    dic = result.json()
    data_dic = dic.get('result')
    


    #KEYS TO KEEP 
    main_keys = ['Gtrends1', 'Instagram', 'Revenue', 'Twitter',  'instagram_posts', 'Gtrends', 'AlexaUniqueVisits', 'AlexaTotalVisits', 'instagram_likes']
     

    df_dic = defaultdict(pd.DataFrame)
    if len(data_dic.keys()) == 0: 
      df_dic[ticker_upper] = pd.DataFrame()
      return 
    dates = []
    #build dict of data frames
    for key in data_dic.keys():
      #check if key is one of wanted 
      if key in main_keys:
        
      
      
        if insta == False: 
            if key.lower().startswith('insta'): 
                continue 
        if len(data_dic[key][0]['series']) != 0: 
          ser = data_dic[key][0]['series']
        else:
          try: 
           
            ser =  data_dic[key][0]['custom']['series']
          except: 
            continue
          


        #build DF and append 
        
        ser_len = len(ser)
        ticker = [ticker_upper.upper()] *ser_len
        #initial DF

        if key != 'Revenue':
            
        #series DF 
          ser_df = pd.DataFrame(ser, columns=['date',data_dic[key][0]['yaxis']])
          ser_df['date'] = ser_df['date'].apply(lambda x: datetime.fromtimestamp(x/ 1000,tz =timezone.utc).date())
          ser_df['date'] = pd.to_datetime(ser_df['date']) 
          quart = ser_df['date'].dt.quarter.values[-1]
          dates.append((ser_df['date'].values[-1], quart))
          #rename cols 
          if key =='Gtrends':
                ser_df = ser_df.rename(columns = {'Google Trends': 'Google Trends_Global'})
          if key == 'Gtrends1':
            ser_df = ser_df.rename(columns ={'Google Trends': 'Google Trends_US'})
          if key.startswith('Alexa'):
            ser_df.columns = [x.split('(')[0].strip() if x.startswith('Alexa') else x for x in ser_df.columns]
          df_dic[key] = ser_df
        else:

          #last_reported_Q
          last_q = datetime.fromtimestamp(data_dic[key][0]['custom']['last_reported_Q']/1000,tz =timezone.utc).date() 
          #Series DF for revenue
          date_rev = [[datetime.fromtimestamp(d['x']/1000,tz =timezone.utc).date(),d['y']] for d in data_dic[key][0]['series']]
          ser_df = pd.DataFrame(date_rev, columns=['date',data_dic[key][0]['yaxis']])
          #actual vs estimate columm 
          ser_df.loc[ser_df['date'] <=last_q,'actual_flag' ] = 'A'
          ser_df.loc[ser_df['date'] > last_q,'actual_flag' ] = 'E'
          ser_df['date'] = pd.to_datetime(ser_df['date'])
          #combine and append 
          ser_df['ticker'] = ticker 
          df_dic[key] = ser_df

                
          


      else: 
        pass

    #Appending Quarterly dates 
    def append_q_dates(df_dic,ticker = ticker_upper,dates = dates):
      df_lst = []
      import calendar 
      #getting end dates 
      url_dt = f'https://app.sentieo.com/api/fetch_new_model_data/?model_source=snt-std&ticker={ticker_upper.lower()}&ptype=fy&stmttype=is&currency=usd&units=M&counter=3&_=1598631942804&loc=app&csrfmiddlewaretoken=zTbd194AUeESRMbIgZCzNzxENReRjzsn&apikey=0e17c30b4fd5f6a39dae2718375b2885f902f18fc661c36fa79ec55369ba2b5c'
      dts_rslt = requests.get(url_dt)
      if dts_rslt.status_code != 200: 
        print(f'Warning: API call needs adjustment, potentially invalid end dates call: {dts_rslt.status_code}')
      else: 
        prd_end_dic = dts_rslt.json()
        prd_end_dts = prd_end_dic.get('result')
        dts = list(prd_end_dts['col_metadata'][0]['perenddt']['data'].values())
        dts.sort()
        prd_end =dts[-1][:3]
      
    #get revenue dates  
      max_date = max(dates,key = lambda x: x[0])
      quart_rev = df_dic['Revenue']['date'].dt.quarter.values[-1]
      rev_dts = df_dic['Revenue']['date'].tolist()
      last_dt = rev_dts[-1]
      if last_dt < max_date[0] and quart_rev != max_date[1]: 
            #build data for missing quarters
            start_dt = last_dt 
            end_dt = max_date[0]
            dates = pd.date_range(start_dt, end_dt, freq = '3M')
            dates = [date for date in dates if date not in rev_dts]
            flags = ['E'] *len(dates)
            ticker = [ticker]*len(dates)
            append_df = pd.DataFrame({'date': dates, 'actual_flag': flags, 'ticker': ticker})
            df_dic['Revenue'] = df_dic['Revenue'].append(append_df, ignore_index = True)  
      #seperate dicts 
      #chunked dicts 
      sep_dic = defaultdict(dict)

      daily_dic = {key: val for key,val in df_dic.items() if ((val['date'] - val['date'].shift(1)).mode()[0] == timedelta(days =1))}
      weekly_dic = {key: val for key,val in df_dic.items() if key not in daily_dic.keys() and key != 'Revenue'}
      sep_dic['weekly_df'] = weekly_dic
      sep_dic['daily_df'] = daily_dic
      

      df_dic['Revenue']['quarter'] = pd.PeriodIndex(df_dic['Revenue']['date'].values, freq = f'Q-{prd_end}')
      df_dic['Revenue']['quarter_end_dates'] = pd.to_datetime(df_dic['Revenue']['date'])
      df_dic['Revenue'] = df_dic['Revenue'].drop('date',axis = 1)
      
           
      
      del_keys = [] 
      
      #loop through all dfs 
      for key in sep_dic.keys():
        if not sep_dic[key]: 
            del_keys.append(key)
        try:
            
            #combine into a df 
            sep_dic[key] = pd.concat([val.set_index('date') for val in sep_dic[key].values()],axis = 1)
            sep_dic[key] = sep_dic[key].reindex(sorted(sep_dic[key].columns), axis=1)
            #build quarters 
            sep_dic[key]['quarter'] = pd.PeriodIndex(sep_dic[key].index.values, freq = f'Q-{prd_end}')
            #merge with rev quarters 
            #trend names
            trend_names = sep_dic[key].select_dtypes(include = ['float64','int64']).columns.tolist()
            #append quarter_end_dates 
            sep_dic[key] = sep_dic[key].reset_index()
            sep_dic[key] = pd.merge(sep_dic[key],df_dic['Revenue'][['quarter_end_dates','quarter']],on = 'quarter',how ='right') 
            sep_dic[key] = sep_dic[key].dropna(subset = trend_names,how = 'all')
            sep_dic[key] = sep_dic[key].set_index('date')
        except Exception as e: 
            print(f'Error while creating initial data frame for {key}:', e)
            continue
      df_dic['Revenue'] = df_dic['Revenue'].set_index('quarter')
      sep_dic['revenue_df'] = df_dic['Revenue']
      [sep_dic.pop(del_key,None) for del_key in del_keys]
      return sep_dic
      

    sep_dic = append_q_dates(df_dic)
    return sep_dic

    


# In[303]:


#tabloo.show(f)


# In[304]:


#qgrid.show_grid(f.drop('quarter',axis = 1),show_toolbar=True)


# # Helper Functions 

# ## Calculating YOY Changes 

# ### 1 year

# In[305]:




def daily_yoy(ser):
  ser = ser.sort_index(ascending= False)
  from dateutil.relativedelta import relativedelta
  curr_lst = []
  curr_t_lst = []
  #remove duplicated indecies 
  ser = ser[~ser.index.duplicated(keep='first')]
  for i in range(len(ser)):
    curr = ser.iloc[i]
    curr_t = ser.index[i]
    
    #missing current point
    if curr == np.nan:
      #pass 
      continue 
    else: 
      #refrence date 365 days before 

      ref_dt = curr_t - relativedelta(years = 1)
      nearest_d = ser.index[ser.index.get_loc(ref_dt,method='nearest')]
      while (len(ser.loc[str(nearest_d)]) == 0 and nearest_d > ser.index[-1]):
        nearest_d -= relativedelta(days = 1) 
        
      #we reach the end
      if nearest_d <= ser.index[-1]:
        break
      #print(ref_dt)
      ref_pt = ser.loc[str(nearest_d)][0]
      #find the pct_change 
      diff = ((curr - ref_pt) / ref_pt) *100
      #if both current and refrence points are zero
      if curr == 0 and ref_pt ==0:
        diff = 0
      #if ref_pt ==0 and curr!=0 
      if curr!= 0 and ref_pt == 0:
        diff = 100  

      
      
      #print(curr)
      curr_lst.append(diff)
      #print(curr_lst)
      #print(curr_lst)
      curr_t_lst.append(curr_t)
  
  return pd.Series(data = curr_lst, index = curr_t_lst, name = ser.name)



      
   
def weekly_yoy(ser):
  #remove duplicated indecies 
  ser = ser[~ser.index.duplicated(keep='first')]
  return ser.pct_change(52) * 100



def quarterly_yoy(df):
    
  '''DF Given must contain quarter end dates,quarter and have date column as its index''' 
  from dateutil.relativedelta import relativedelta
  #get current quarter 
  #remove duplicated indecies 
  df = df[~df.index.duplicated(keep='first')]
  curr_df = df.drop('quarter',axis = 1)
  curr_q = curr_df.loc[:,'quarter_end_dates'][-1]
  curr_d = curr_df.index[-1]

  #current quarter values  
  curr_vals = curr_df.loc[curr_df['quarter_end_dates'] == curr_q]

  #current quarter change calc 
  prior_q = curr_q - relativedelta(years = 1)
  prior_d = curr_d -relativedelta(years = 1)
  
  nearest_d  = curr_df.index[curr_df.index.get_loc(prior_d,method ='nearest')]
  prior_vals =curr_df.truncate(after =  str(nearest_d))
  prior_vals = prior_vals[prior_vals['quarter_end_dates'] == prior_q]

  #in case of prior year being empty 
  if len(prior_vals) < len(curr_vals): 
 
    #check a week ahead 
    i = 1
    while(i != 7): 
      nearest_d += relativedelta(days = 1)
      prior_vals = curr_df.truncate(after =  str(nearest_d))
      prior_vals = prior_vals[prior_vals['quarter_end_dates'] == prior_q]

      if len(prior_vals) == len(curr_vals): 
        break
      i+=1 
    #if still nothing: cap at a 100 
    if len(prior_vals) == 0: 
      curr_chg = 100 

    
    else: 
      curr_chg = ((curr_vals.sum() - prior_vals.sum())/ prior_vals.sum()) * 100
      curr_chg = curr_chg[0]
  else: 
    #check how many we have 
    curr_chg = ((curr_vals.sum() - prior_vals.sum())/ prior_vals.sum()) * 100
    curr_chg = curr_chg[0]
      
    
  
    
  


  #other quarter calc 

  df = df.groupby(['quarter','quarter_end_dates']).sum()
  df = df.pct_change(4) * 100
  if df.isna().all()[0] == True:
    return df 
  df.loc[(slice(None),curr_q),:] =curr_chg
  return df 



def monthly_yoy(ser):
  from dateutil.relativedelta import relativedelta
  #remove duplicated indecies 
  ser = ser[~ser.index.duplicated(keep='first')]
  #current handeling 
  #current date 
  curr_date = ser.index[-1]
  last_day = curr_date.day 
  curr_vals = ser.loc[f'{curr_date.year}-{curr_date.month}']

  #previous year vals 
  prior_date = curr_date - relativedelta(years = 1)
  ser_dt = ser[f'{prior_date.year}-{prior_date.month}'].copy()
  nearest_dt = ser_dt.index[ser_dt.index.get_loc(prior_date,method='nearest')]
  prior_vals = ser.truncate(after =  str(nearest_dt), before= f'{prior_date.year}-{prior_date.month}-01')

  if len(prior_vals) < len(curr_vals): 
    #look a week ahead 
    i = 1
    while(i != 7): 
      nearest_dt += relativedelta(days = 1)
      prior_vals = ser.truncate(after = str(nearest_dt), before = f'{prior_date.year}-{prior_date.month}-01')
      if len(prior_vals) == len(curr_vals): 
        break
      i+=1 
    #if still nothing: cap at a 100 
    if len(prior_vals) == 0: 
      curr_chg = 100 
    else: 
      curr_chg = ((curr_vals.values.sum() - prior_vals.values.sum())/ prior_vals.values.sum()) * 100
  else: 
    curr_chg = ((curr_vals.values.sum() - prior_vals.values.sum())/ prior_vals.values.sum()) * 100
  
  #other months 
  ser = ser.resample('1M').sum()
  ser = ser.pct_change(12) * 100 
  ser.iloc[-1] = curr_chg
  return ser 

    

def weekly_ser_daily(ser,ind):
    '''ind is Gtrends'dates returned by get_dfs. ser is the trend of interest with its date as the index'''
    week_dt = ind
    week_dt = week_dt.dropna().sort_values()
    #remove duplicated indecies 
    ser = ser[~ser.index.duplicated(keep='first')]
    #get daily_yoy_change 
    ser = daily_yoy(ser)
    ser = ser.sort_index()
    #truncate any dates without a corresp week 
    ser = ser.truncate(after = str(week_dt[-1]))
    ser_dt = ser.index 
    #searchsorted 
    index = week_dt.searchsorted(ser_dt,side = 'left')
    week_dt = week_dt[index]
    #make a df
    df = pd.DataFrame({ser.name:ser, 'date':week_dt})
    #aggregate by weeks and take the mean 
    df = df.groupby('date').mean()
    
    return df 





# ### 2 year

# In[306]:


def daily_2yoy(ser):
  #remove duplicated indecies 
  ser = ser[~ser.index.duplicated(keep='first')]
  ser = ser.sort_index(ascending= False)
  from dateutil.relativedelta import relativedelta
  curr_lst = []
  curr_t_lst = []
  
  for i in range(len(ser)):
    curr = ser.iloc[i]
    curr_t = ser.index[i]
    
    #missing current point
    if curr == np.nan:
      #pass 
      continue 
    else: 
      #refrence date 730 days before 

      ref_dt = curr_t - relativedelta(years = 2)
      nearest_d = ser.index[ser.index.get_loc(ref_dt,method='nearest')]
      while (len(ser.loc[str(nearest_d)]) == 0 and nearest_d > ser.index[-1]):
        nearest_d -= relativedelta(days = 1) 
        
      #we reach the end
      if nearest_d <= ser.index[-1]:
        break
      #print(ref_dt)
      ref_pt = ser.loc[str(nearest_d)][0]
      #find the pct_change 
      diff = ((curr - ref_pt) / ref_pt) *100
      #if both current and refrence points are zero
      if curr == 0 and ref_pt ==0:
        diff = 0
      #if ref_pt ==0 and curr!=0 
      if curr!= 0 and ref_pt == 0:
        diff = 100  

      
      
      #print(curr)
      curr_lst.append(diff)
      #print(curr_lst)
      #print(curr_lst)
      curr_t_lst.append(curr_t)
  
  return pd.Series(data = curr_lst, index = curr_t_lst, name = ser.name)


def weekly_2yoy(ser):
  #remove duplicated indecies 
  ser = ser[~ser.index.duplicated(keep='first')]
  return ser.pct_change(104) * 100



# ## Stats Calculations

# In[307]:


def stats_df(df, drop_dates = []): 
    df = df.reset_index()
    df2 = df.copy()
    from collections import defaultdict
    stats_dic = defaultdict(list)
    cols = []
        #date dropping 
    if len(drop_dates) != 0:
        
     
        #if in m/Y format or Y/m
        if len(re.findall(r'^\d{2}/\d{4}$',drop_dates[0])) != 0 or len(re.findall(r'^\d{4}/\d{2}$',drop_dates[0])) !=0:
            df = df.drop(df.loc[(df.index.strftime('%m/%Y').isin(drop_dates))|(df.index.strftime('%Y/%m').isin(drop_dates)) ,:].index)
        
        #number format 
        if len(re.findall(r'^\d.$', drop_dates[0])) != 0:
            max_length = len(df[df.actual_flag == 'A'])
            n = int(drop_dates[0])
            if n > max_length:
                df = df 
            else:
                to_drop = df.loc[df.actual_flag == 'A'][:-n].index
                df = df.drop(to_drop)
 

            

    
    to_skip = ['Revenue_yoy_change','FQ','Revenue_2Ychange','ticker','actual_flag','quarter_end_dates','quarter']
    to_skip.extend([x for x in df.columns if '2Yaccel' in x or 'accel' in x])
    for column in df.columns: 
        if column in to_skip: 
            continue
        else:
            try:
                
                curr_df = df.loc[(df['actual_flag'] == 'A') & (df[column] != np.inf) & (df[column] != np.nan), [column,'Revenue_yoy_change','Revenue_2Ychange']].dropna()
                est_df = df.loc[(df['actual_flag'] == 'E') & (df[column] != np.inf) & (df[column] != np.nan), [column,'Revenue_yoy_change','Revenue_2Ychange']].dropna()
                    
                #get x-y arrays
                if len(re.findall(r'^.*_2Y.*$',column)) !=0: 
                    y = np.array(curr_df['Revenue_2Ychange'].values,dtype = 'float64')
                    y2 = np.array(curr_df['Revenue_2Ychange'].values[:-1],dtype = 'float64')
                else:
                    y = np.array(curr_df['Revenue_yoy_change'].values,dtype = 'float64')
                    y2 = np.array(curr_df['Revenue_yoy_change'].values[:-1],dtype = 'float64')
                x = np.array(curr_df[column].values,dtype = 'float64')
                x2 = np.array(curr_df[column].values[:-1],dtype ='float64')
                
                #slope/intercept calculations 
                slope, intercept = np.polyfit(x,y,deg = 1)
                r_sq, p_value = stats.pearsonr(x,y)
                adj_r = (1-(1-r_sq**2) * ((curr_df.shape[0] - 1) / (curr_df.shape[0] - 1 - 1)))

                #excluding
                slope2,intercept2 = np.polyfit(x2,y2,deg = 1)
                r_sq2, p_value2 = stats.pearsonr(x2,y2)
                adj_r2 = (1-(1-r_sq2**2) * ((curr_df.shape[0] - 1) / (curr_df.shape[0] - 1 - 1)))

                stats_dic['slope_including_FQ0'].append(slope)
                stats_dic['intercept_including_FQ0'].append(intercept)
                stats_dic['R^2_including_FQ0'].append(r_sq**2)
                stats_dic['PValue_including_FQ0'].append(p_value)
                stats_dic['Adj_R^2_including_FQ0'].append(adj_r)

                stats_dic['slope_excluding_FQ0'].append(slope2)
                stats_dic['intercept_excluding_FQ0'].append(intercept2)
                stats_dic['R^2_excluding_FQ0'].append(r_sq2**2)
                stats_dic['PValue_excluding_FQ0'].append(p_value2)
                stats_dic['Adj_R^2_excluding_FQ0'].append(adj_r2)
                
                
                #forecasts
                if len(est_df) == 0:
                  stats_dic['FQ1_Forecast_including_FQ0'].append(np.nan)
                  stats_dic['FQ1_Forecast_excluding_FQ0'].append(np.nan)
                  stats_dic['FQ2_Forecast_including_FQ0'].append(np.nan)
                  stats_dic['FQ2_Forecast_excluding_FQ0'].append(np.nan)

                  stats_dic['FQ1_Surp_including_FQ0'].append(np.nan)
                  stats_dic['FQ1_Surp_excluding_FQ0'].append(np.nan)
                  stats_dic['FQ2_Surp_including_FQ0'].append(np.nan)
                  stats_dic['FQ2_Surp_excluding_FQ0'].append(np.nan)

                elif len(est_df) == 1:
                  #we have one quarter worth 
                  forecast_incl = est_df[column].values[0] * slope + intercept
                  forecast_excl = est_df[column].values[0] * slope2 + intercept2
                  stats_dic['FQ1_Forecast_including_FQ0'].append(forecast_incl)
                  stats_dic['FQ1_Forecast_excluding_FQ0'].append(forecast_excl)
                  stats_dic['FQ2_Forecast_including_FQ0'].append(np.nan)
                  stats_dic['FQ2_Forecast_excluding_FQ0'].append(np.nan)
                  if len(re.findall(r'^.*_2Y.*$',column)) !=0:
                        act_forecast = est_df['Revenue_2Ychange'].values[0]
                  else:
                    act_forecast = est_df['Revenue_yoy_change'].values[0]

                  stats_dic['FQ1_Surp_including_FQ0'].append(((forecast_incl- act_forecast)/ np.abs(act_forecast))*100)
                  stats_dic['FQ1_Surp_excluding_FQ0'].append(((forecast_excl- act_forecast)/ np.abs(act_forecast))*100)

                  stats_dic['FQ2_Surp_including_FQ0'].append(np.nan)
                  stats_dic['FQ2_Surp_excluding_FQ0'].append(np.nan)
                else:
                  forecast_incl = est_df[column].values[0] * slope + intercept
                  forecast_excl = est_df[column].values[0] * slope2 + intercept2

                  forecast2_incl = est_df[column].values[1] * slope + intercept
                  forecast2_excl = est_df[column].values[1] * slope2 + intercept2

                  stats_dic['FQ1_Forecast_including_FQ0'].append(forecast_incl)
                  stats_dic['FQ1_Forecast_excluding_FQ0'].append(forecast_excl)
                  stats_dic['FQ2_Forecast_including_FQ0'].append(forecast2_incl)
                  stats_dic['FQ2_Forecast_excluding_FQ0'].append(forecast2_excl)
                  if len(re.findall(r'^.*_2Y.*$',column)) !=0:
                        act_forecast = est_df['Revenue_2Ychange'].values[0]
                        act_forecast2 = est_df['Revenue_2Ychange'].values[1]
                  else:
                    act_forecast = est_df['Revenue_yoy_change'].values[0] 
                    act_forecast2 = est_df['Revenue_yoy_change'].values[1]

                  stats_dic['FQ1_Surp_including_FQ0'].append(((forecast_incl- act_forecast)/ np.abs(act_forecast))*100)
                  stats_dic['FQ1_Surp_excluding_FQ0'].append(((forecast_excl- act_forecast)/ np.abs(act_forecast))*100)
                  stats_dic['FQ2_Surp_including_FQ0'].append(((forecast2_incl- act_forecast)/ np.abs(act_forecast))*100)
                  stats_dic['FQ2_Surp_excluding_FQ0'].append(((forecast2_excl- act_forecast)/ np.abs(act_forecast))*100)

            
            except Exception as e:
                print(e)
                continue
            cols.append(column)
    df2 = df2.set_index('FQ')
    for key, val in stats_dic.items(): 
        df2.loc[key,cols] = val 
    return df2
    
        
            
     


# ## Display Function 

# In[308]:


def quart_num(df,to_display, quart = True): 
    n = to_display
    if quart == True: 
        max_length = len(df[df.actual_flag == 'A'])
        if n > max_length:
            return df 
        to_drop = df.loc[df.actual_flag == 'A'][:-n].index
        return df.drop(to_drop)
    else: 
        dff = df.loc[df.actual_flag == 'A']
        max_length = len(dff.quarter.unique())
        if n > max_length: 
            return df 
        else: 
            keep = dff.quarter.unique()[-n:]
            df = df.loc[(df.quarter.isin(keep))|(df.actual_flag == 'E')]
            return df 


    


# ## Plotting Functions 

# In[309]:


def scatter_matrix(df): 
    g = sns.PairGrid(df, hue="actual_flag",height = 5.5)
    g.map_diag(plt.hist)
    g.map_offdiag(sns.scatterplot, s= 150)
    g.add_legend()
    plt.show()
    
def ts_plots(df,cols_dict,key,b,ticker):
    #get decomp ser
    decomp_rev2 =sm.tsa.seasonal_decompose(df.iloc[:,4],model='additive') 
    decomp_rev = sm.tsa.seasonal_decompose(df.iloc[:,3],model='additive')
    decomp = sm.tsa.seasonal_decompose(df.iloc[:,0],model='additive')
    decomp2 = sm.tsa.seasonal_decompose(df.iloc[:,1],model='additive')
    decomp3 = sm.tsa.seasonal_decompose(df.iloc[:,2],model='additive')
    print('\n')
    print('Time Series Decomposition:')
    print('\n')
    #plotting 
    fig,axs = plt.subplots(6,1,figsize = (17,32))
    
    
    #observed
    #1Y
    obs_lst = [decomp.observed,decomp2.observed, decomp3.observed,decomp_rev.observed,decomp_rev2.observed]
    host = axs[0]
    ax2 = host.twinx()
    host.set_ylabel(cols_dict[key][3])
    ax2.set_ylabel(f'{b}')

    p1, =host.plot(obs_lst[3], color = 'blue',label = cols_dict[key][3])
    p2, = ax2.plot(obs_lst[0], color = 'black', label = cols_dict[key][0])
    p3, = ax2.plot(obs_lst[1],color = 'red', label = cols_dict[key][1])

    axs[0].set_title(f'\n {ticker} \n \n Observed {b} Data Over Time \n',fontsize =15)
    host.legend(handles = [p1, p2,p3] ,loc = 2, fontsize = 'medium')
    host.yaxis.set_ticks_position('right')
    host.yaxis.set_label_position('right')
    ax2.spines['right'].set_position(('outward', 60))

    align_yaxis([host, ax2])
    
    #2Y 
    host = axs[1]
    ax2 = host.twinx()
    host.set_ylabel(cols_dict[key][4])
    ax2.set_ylabel(f'{b}_2Ychange')
    
    p1, = host.plot(obs_lst[4], color = 'blue',label = cols_dict[key][4])
    p2, = ax2.plot(obs_lst[2], color = 'green', label = cols_dict[key][2])
    
    axs[1].set_title(f'\n {ticker} \n \n Observed {b} Data 2YChange Over Time \n',fontsize =15)
    host.legend(handles = [p1, p2] ,loc = 2, fontsize = 'medium')
    host.yaxis.set_ticks_position('right')
    host.yaxis.set_label_position('right')
    ax2.spines['right'].set_position(('outward', 60))
    
    align_yaxis([host, ax2])
    
    #Trend
    #1Y
    trend_lst = [decomp.trend,decomp2.trend,decomp3.trend,decomp_rev.trend,decomp_rev2.trend]
    host = axs[2]
    ax2 = host.twinx()
    host.set_ylabel(cols_dict[key][3])
    ax2.set_ylabel(f'{b}')

    p1, =host.plot(trend_lst[3], color = 'blue',label = cols_dict[key][3])
    p2, = ax2.plot(trend_lst[0], color = 'black', label = cols_dict[key][0])
    p3, = ax2.plot(trend_lst[1],color = 'red', label = cols_dict[key][1])
    


    axs[2].set_title(f'\n \n {b} Trends (Long Term Directions) Over Time \n',fontsize =15)
    host.legend(handles = [p1, p2,p3] ,loc = 2, fontsize = 'medium')
    host.yaxis.set_ticks_position('right')
    host.yaxis.set_label_position('right')
    ax2.spines['right'].set_position(('outward', 60))
    align_yaxis([host, ax2])
    
    
    #2y 
    host = axs[3]
    ax2 = host.twinx()
    host.set_ylabel(cols_dict[key][4])
    ax2.set_ylabel(f'{b}_2Ychange')
    
    p1, = host.plot(trend_lst[4], color = 'blue',label = cols_dict[key][4])
    p2, = ax2.plot(trend_lst[2], color = 'green', label = cols_dict[key][2])
    
    axs[3].set_title(f'\n \n {b} Trends (Long Term Directions) 2YChange Over Time \n',fontsize =15)
    host.legend(handles = [p1, p2] ,loc = 2, fontsize = 'medium')
    host.yaxis.set_ticks_position('right')
    host.yaxis.set_label_position('right')
    ax2.spines['right'].set_position(('outward', 60))
    
    align_yaxis([host, ax2])
    
    #seasonality
    #1Y
    seas_lst = [decomp.seasonal,decomp2.seasonal,decomp3.seasonal,decomp_rev.seasonal,decomp_rev2.seasonal]

    host = axs[4]
    ax2 = host.twinx()
    host.set_ylabel(cols_dict[key][3])
    ax2.set_ylabel(f'{b}')

    p1, =host.plot(seas_lst[3], color = 'blue',label = cols_dict[key][3])
    p2, = ax2.plot(seas_lst[0], color = 'black', label = cols_dict[key][0])
    p3, = ax2.plot(seas_lst[1],color = 'red', label = cols_dict[key][1])


    axs[4].set_title(f'\n \n {b} Seasonality (Repeating Short Term Cycles) Over Time \n',fontsize =15)
    host.legend(handles = [p1, p2,p3] ,loc = 2, fontsize = 'medium')
    host.yaxis.set_ticks_position('right')
    host.yaxis.set_label_position('right')
    ax2.spines['right'].set_position(('outward', 60))
    align_yaxis([host, ax2])
    
    
    #2Y
    host =axs[5]
    ax2 = host.twinx()
    host.set_ylabel(cols_dict[key][4])
    ax2.set_ylabel(f'{b}_2Ychange')
    
    p1, = host.plot(seas_lst[4], color = 'blue',label = cols_dict[key][4])
    p2, = ax2.plot(seas_lst[2], color = 'green', label = cols_dict[key][2])
    
    axs[5].set_title(f'\n \n {b} Seasonality (Repeating Short Term Cycles) 2YChange Over Time \n',fontsize =15)
    host.legend(handles = [p1, p2] ,loc = 2, fontsize = 'medium')
    host.yaxis.set_ticks_position('right')
    host.yaxis.set_label_position('right')
    ax2.spines['right'].set_position(('outward', 60))
    
    align_yaxis([host, ax2])
    plt.subplots_adjust(left=0.125, 
                    right=0.9, 
                     
                    wspace=0.3, 
                    hspace=0.6)
    plt.show()
    
def resid_plts(df,cols_dict,key,b,ticker):
    
    decomp_rev = sm.tsa.seasonal_decompose(df_t.iloc[:,3],model='additive')
    decomp = sm.tsa.seasonal_decompose(df_t.iloc[:,0],model='additive')
    decomp2 = sm.tsa.seasonal_decompose(df_t.iloc[:,1],model='additive')
    decomp3 = sm.tsa.seasonal_decompose(df_t.iloc[:,2],model='additive')
    #print means
    print('\n')
    print('Residual Decomposition:')
    print('\n')
    print(f'Revenue Noise Mean:{decomp_rev.resid.mean():.3f}')
    print(f'{cols_dict[key][0]} Mean:{decomp.resid.mean():.3f}')
    print(f'{cols_dict[key][1]} Mean:{decomp2.resid.mean():.3f}')
    print(f'{cols_dict[key][2]} Mean:{decomp3.resid.mean():.3f}')
    #plotting 
    plt.rcParams['figure.figsize'] = 12,6
    plt.hist(decomp.resid,color = 'black')
    plt.hist(decomp2.resid,color ='red')
    plt.hist(decomp_rev.resid,color = 'blue')
    plt.hist(decomp3.resid,color = 'green')
    plt.title(f'\n {ticker} \n \n Histograms of the Noise Component in the {b} data \n',fontsize =15)
    plt.legend(cols_dict[key],loc = 2, fontsize = 'medium')
    plt.show()


# In[310]:


def line_plots(df,cols_dict,key,b,ticker,hit_rates,accel_dict,wk_dict,mo_cols = [], df_weekly = None, df_monthly = None):
    #get ser 
    df = df.reset_index()
    df =df.set_index('quarter_end_dates')
    df = df.dropna() 
    df = df.sort_index()
    rev = df.loc[:,'Revenue_yoy_change']
    rev2 = df.loc[:,'Revenue_2Ychange']
    trend = df.loc[:,cols_dict[key][0]]
    trend2 = df.loc[:,cols_dict[key][1]]
    trend3 = df.loc[:,cols_dict[key][2]]

    print('\n')
    print('1Y Plots:')
    print('\n')
    #plotting 
    fig,axs = plt.subplots(1,1,figsize = (24,9))
    
    
    #observed
    #1Y
    host = axs
    ax2 = host.twinx()
    host.set_ylabel(cols_dict[key][3])
    ax2.set_ylabel(f'{b}')

    p1, =host.plot(rev, color = 'black',label = cols_dict[key][3])
    p2, = ax2.plot(trend, color = 'blue', label = cols_dict[key][0])
    p3, = ax2.plot(trend2,color = 'red', label = cols_dict[key][1])

    host.set_title(f'\n {ticker} \n \n Observed {b} Data Over Time \n',fontsize =20)
    host.legend(handles = [p1, p2,p3] ,loc = 2, fontsize = 'medium')
    host.yaxis.set_ticks_position('right')
    host.yaxis.set_label_position('right')
    ax2.spines['right'].set_position(('outward', 60))

    align_yaxis([host, ax2])
    
    
    #annotate hit rates 
    host.text(0.5,-0.1,s =f'Hit Rates: \n{cols_dict[key][0]} Hit Rate: {hit_rates[0]:.1f}% \n{cols_dict[key][1]} Hit Rate: {hit_rates[1]:.1f}%',color = 'black',size = 20, ha='center', va='bottom',transform=fig.transFigure )
    
    plt.show()
      
    print('\n')
    print('2Y Plots:')
    print('\n')
    #2Y  
    fig2,axs2 = plt.subplots(1,1,figsize = (24,9))
    host2 = axs2
    ax2 = host2.twinx()
    host2.set_ylabel(cols_dict[key][4])
    ax2.set_ylabel(f'{b}_2Ychange')
    
    p1, = host2.plot(rev2, color = 'black',label = cols_dict[key][4])
    p2, = ax2.plot(trend3, color = 'blue', label = cols_dict[key][2])
    
    host2.set_title(f'\n {ticker} \n \n Observed {b} Data 2YChange Over Time \n',fontsize =20)
    host2.legend(handles = [p1, p2] ,loc = 2, fontsize = 'medium')
    host2.yaxis.set_ticks_position('right')
    host2.yaxis.set_label_position('right')
    ax2.spines['right'].set_position(('outward', 60))
    
    align_yaxis([host2, ax2])
    host2.text(0.46,-0.06,s=f'Hit Rates: \n{cols_dict[key][2]} Hit Rate: {hit_rates[2]:.1f}% \n\n',color = 'black',size = 20, ha='center', va='bottom',transform=fig.transFigure )
    plt.show()
    print('\n')
    
    #weekly plots 
    if df_weekly is not None: 
        df_weekly = df_weekly.sort_index()
        rev_wk = df_weekly.loc[:,wk_dict[key][1]]
        trend_wk = df_weekly.loc[:,wk_dict[key][0]]
        print('\n')
        print('Weekly Plots:')
        print('\n')
        fig3,axs3 = plt.subplots(1,1,figsize = (24,9))
        host3 = axs3
        ax3 = host3.twinx()
        host3.set_ylabel(wk_dict[key][1])
        ax3.set_ylabel(f'{b}_Weekly')
        
        
        p1, =   host3.plot(rev_wk, color = 'black',label = wk_dict[key][1])
        p2, = ax3.plot(trend_wk, color = 'blue', label = wk_dict[key][0])
        
        host3.set_title(f'\n {ticker} \n \n Observed {b} Data Weekly Change Over Time \n',fontsize =20)
        host3.legend(handles = [p1, p2] ,loc = 2, fontsize = 'medium')
        host3.yaxis.set_ticks_position('right')
        host3.yaxis.set_label_position('right')
        ax3.spines['right'].set_position(('outward', 60))
        align_yaxis([host3, ax3])
        plt.show()
        
        
    


# In[311]:


def line_plts_plotly(df,b,cols_dict,key,hit_rates,accel_dict, wk_cols = [],mo_cols=[],df_weekly = None, df_monthly = None): 
    df = df.reset_index()
    df =df.set_index('quarter_end_dates')
    df = df.dropna() 
    df = df.sort_index()
    rev = df.loc[:,'Revenue_yoy_change']
    rev2 = df.loc[:,'Revenue_2Ychange']
    trend = df.loc[:,cols_dict[key][0]]
    trend2 = df.loc[:,cols_dict[key][1]]
    trend3 = df.loc[:,cols_dict[key][2]]
    #1Y plot
    fig = go.Figure()
    #rev
    fig.add_trace(go.Scatter(x = df.index, y = rev,name = 'Revenue_yoy_change',
                            mode = 'lines+markers',hovertext= df.FQ,yaxis = 'y',line = dict(color = 'black')))
    #trends
    fig.add_trace(go.Scatter(x = df.index, y = trend,name = cols_dict[key][0],
                            mode = 'lines+markers',hovertext= df.FQ,yaxis = 'y2',line = dict(color = 'blue')))
    fig.add_trace(go.Scatter(x = df.index, y = trend2,name = cols_dict[key][1],
                            mode = 'lines+markers',hovertext= df.FQ,yaxis = 'y2',line = dict(color = 'red')))
    
    #axis 
    #alignment yaxis 
    data = [{'x':df.index, 'y':rev},{'x':df.index, 'y':trend}]
    axis_names = ['Revenue', f'{b}']
    axis_colors = ['black','blue']
    offset = 0.075
    tick_num = 5
    template = 'plotly'
    y_axis_dict,right_y_position =  get_y_axis_dict([data['y'] for data in data], axis_names = axis_names, tick_num= tick_num,axis_colors=axis_colors,offset=offset)
    print(y_axis_dict)
    #layouts 
    #xaxis 
    fig.update_layout(xaxis=dict(domain=[0, right_y_position],
                            rangeslider_visible=True,type ='date',
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=3, label="3m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")]))))
    
    #yaxis 
    fig.update_layout(y_axis_dict)
    
    #figure
    fig.update_layout(
    title_text=f"Revenue vs. {b} Changes Over Time",
    width=1050,height = 600,legend=dict(
    y=1.21,
    xanchor="right",
    x=1),template =template)
    
    fig.add_annotation(text=f'Hit Rates: <br>{cols_dict[key][0]} Hit Rate: {hit_rates[0]:.1f}% <br>{cols_dict[key][1]} Hit Rate: {hit_rates[1]:.1f}%',
                       showarrow=False, visible = True,xref = 'paper',yref = 'paper',
                       bordercolor='blue',
                borderwidth=3,y = -0.53,x = 0.47,align = 'left', width = 450,height = 55,font = dict(size = 13),
                  xshift = 1)
    
    fig.show()
    
    #2Y plots 
    fig2 = go.Figure()
    #rev 
    #rev
    fig2.add_trace(go.Scatter(x = df.index, y = rev2,name = 'Revenue_2Ychange',
                            mode = 'lines+markers',hovertext= df.FQ,yaxis = 'y',line = dict(color = 'black')))
    #trends
    fig2.add_trace(go.Scatter(x = df.index, y = trend3,name = cols_dict[key][2],
                            mode = 'lines+markers',hovertext= df.FQ,yaxis = 'y2',line = dict(color = 'darkorange')))
    
    #axis 
    #align
    data2 = [{'x':df.index, 'y':rev2},{'x':df.index, 'y':trend3}]
    axis_names2 = ['Revenue_2Y', f'{b}_2Y']
    colors2 = ['black','darkorange']
    offset2 = 0.075
    tick_num2 = 5
    template = 'plotly'
    y_axis_dict2, right_y_position2 = get_y_axis_dict([data['y'] for data in data2], axis_names=axis_names2, tick_num=tick_num2,axis_colors=colors2,offset=offset2)
    #layouts 
    #x 
    fig2.update_layout(xaxis=dict(domain=[0, right_y_position2],
                            rangeslider_visible=True,type ='date',
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=3, label="3m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")]))))
    
    #yaxis 
    fig2.update_layout(y_axis_dict2)
    
    #figure
    fig2.update_layout(
    title_text=f"Revenue vs. {b} 2YChanges Over Time",
    width=1050,height = 600,legend=dict(
    y=1.21,
    xanchor="right",
    x=1),template = template)
    try:
        fig2.add_annotation(text=f'Hit Rates: <br>{cols_dict[key][2]} Hit Rate: {hit_rates[2]:.1f}%',
                       showarrow=False, visible = True,xref = 'paper',yref = 'paper',
                       bordercolor='blue',
                borderwidth=3,y = -0.48,x = 0.47,align = 'left', width = 430,height = 40,font = dict(size = 13),
                  xshift = 1)
    except: 
        pass
    fig2.show()
    
    #month/week plots 
    if df_weekly is not None: 
        df_w = df_weekly.reset_index()
        df_w = df_w.set_index('week_date')
        df_w = df_w[wk_cols]
        #plotting 
        fig3 = go.Figure()
         #rev
        fig3.add_trace(go.Scatter(x = df_w.index, y = df_w['Revenue_yoy_change'],name = 'Revenue_yoy_change',
                                mode = 'lines+markers',hovertext= df_w.FQ,yaxis = 'y',line = dict(color = 'black')))
        #trends
        fig3.add_trace(go.Scatter(x = df_w.index, y = df_w[wk_cols[0]],name = wk_cols[0],
                                mode = 'lines+markers',hovertext= df_w.FQ,yaxis = 'y2',line = dict(color = 'blue')))
        
        data = [{'x':df_w.index, 'y':df_w['Revenue_yoy_change'].tolist()},{'x':df_w.index, 'y':df_w[wk_cols[0]].tolist()}]
        names = ['Revenue', f'{b}_weekly']
        colors = ['black','darkorange']
        offset = 0.075
        tick_num = 6
        template = 'plotly'
        y_axis_dict, right_y_position = get_y_axis_dict([data['y'] for data in data], names, tick_num,colors,offset)

        #layouts 
        #x 
        fig3.update_layout(xaxis=dict(domain=[0, right_y_position],
                                rangeslider_visible=True,type ='date',
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all")]))))

        #yaxis 
        fig3.update_layout(yaxis_dict)

        #figure
        fig3.update_layout(
        title_text=f"Revenue vs. {b} Weekly Changes Over Time",
        width=1050,height = 600,legend=dict(
        y=1.21,
        xanchor="right",
        x=1))

        fig3.show()

        if df_month is not None: 
            df_m = df_month.reset_index()
            df_m = df_m.set_index('month_date')
            df_m = df_m[m_cols]
    
            fig4 = go.Figure()
        
            fig4.add_trace(go.Scatter(x = df_m.index, y = df_m['Revenue_yoy_change'],name = 'Revenue_yoy_change',
                                mode = 'lines+markers',hovertext= df.FQ,yaxis = 'y2',line = dict(color = 'black')))
            
            
            fig4.add_trace(go.Scatter(x = df_m.index, y = df_m[mo_cols[0]],name = mo_cols[0],
                                mode = 'lines+markers',hovertext= df.FQ,yaxis = 'y2',line = dict(color = 'blue')))
            
            fig4.add_trace(go.Scatter(x = df_m.index, y = df_m[mo_cols[1]],name = mo_cols[1],
                                mode = 'lines+markers',hovertext= df.FQ,yaxis = 'y2',line = dict(color = 'red')))
            data = [{'x':df_m.index, 'y':df_m['Revenue_yoy_change'].tolist()},{'x':df_m.index, 'y':df_m[wk_cols[0]].tolist()}]
            names = ['Revenue', f'{b}_monthly']
            colors = ['black','darkorange']
            offset = 0.075
            tick_num = 5
            template = 'plotly'
            y_axis_dict, right_y_position = get_y_axis_dict([data['y'] for data in data], names, tick_num,colors,offset)

            #layouts 
            #x 
            fig3.update_layout(xaxis=dict(domain=[0, right_y_position],
                                    rangeslider_visible=True,type ='date',
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=3, label="3m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")]))))

            #yaxis 
            fig3.update_layout(yaxis_dict)

            #figure
            fig3.update_layout(
            title_text=f"Revenue vs. {b} Monthly Changes Over Time",
            width=1050,height = 600,legend=dict(
            y=1.21,
            xanchor="right",
            x=1))

            fig3.show()


# In[312]:


import math
class CalculateTicks:

    def __init__(self, data, tick_num):
        self.negative = False
        self.y_negative_ratio = None
        self.y_positive_ratio = None
        self.y_range_min = None
        self.y_range_max = None

        self.y_min = min(data)
        self.y_max = max(data)
        self.y_range = self.y_max - self.y_min if self.y_min < 0 else self.y_max
        self.y_range = self.y_range * 10000
        self.y_length = len(str(math.floor(self.y_range)))

        self.y_pw10_div = 10 ** (self.y_length - 1)
        self.y_first_digit = math.floor(self.y_range / self.y_pw10_div)
        self.y_max_base = self.y_pw10_div * self.y_first_digit / 10000
        self.y_dtick = self.y_max_base / tick_num
        self.y_dtick_ratio = self.y_range / self.y_dtick





def get_y_axis_dict(y_data, axis_names, tick_num, axis_colors, offset):

    """
    :param offset: the amount in ratio of figure size that the y axes should be offset
    :param axis_colors: a list of plotly colors
    :param tick_num: the number of ticks in the plot
    :param y_data: should be list like with each item being the y_data for plotting
    :param axis_names: the y_axis titles
    :return:
    """

    right_y_position = 1 - (len(y_data) - 1) * offset

    axis_class_list = []
    max_tick_ratio = 0
    for i, data in enumerate(y_data):
        tick_calculation = CalculateTicks(data, tick_num)
        axis_class_list.append(tick_calculation)
        max_tick_ratio = tick_calculation.y_dtick_ratio if tick_calculation.y_dtick_ratio > max_tick_ratio             else max_tick_ratio

    any_negative = False
    for i, tick_calculation in enumerate(axis_class_list):
        if tick_calculation.y_min < 0:
            any_negative = True
            axis_class_list[i].negative = True
            axis_class_list[i].y_negative_ratio = abs(
                tick_calculation.y_min / tick_calculation.y_range) * max_tick_ratio
        else:
            axis_class_list[i].y_negative_ratio = 0
        axis_class_list[i].y_positive_ratio = (tick_calculation.y_max / tick_calculation.y_range) * max_tick_ratio

    global_negative_ratio = 0
    global_positive_ratio = 0
    for i, tick_calculation in enumerate(axis_class_list):
        global_negative_ratio = tick_calculation.y_negative_ratio if tick_calculation.y_negative_ratio                                                                      > global_negative_ratio else global_negative_ratio
        global_positive_ratio = tick_calculation.y_positive_ratio if tick_calculation.y_positive_ratio                                                                      > global_positive_ratio else global_positive_ratio

    global_negative_ratio = global_negative_ratio + 0.1
    for i, tick_calculation in enumerate(axis_class_list):
        if any_negative:
            axis_class_list[i].y_range_min = global_negative_ratio * tick_calculation.y_dtick * -1
        else:
            axis_class_list[i].y_range_min = 0
        axis_class_list[i].y_range_max = global_positive_ratio * tick_calculation.y_dtick

    yaxis_dict = {}
    for i, data in enumerate(y_data):
        if i < 1:
            yaxis_dict = dict(yaxis=dict(
                title=axis_names[i],
                range=[axis_class_list[i].y_range_min, axis_class_list[i].y_range_max],
                dtick=axis_class_list[i].y_dtick,
                titlefont=dict(
                    color=axis_colors[i]
                ),
                tickfont=dict(
                    color=axis_colors[i]
                )
            )
            )

        else:
            y_axis_long = 'yaxis' + str(i + 1)
            yaxis_dict[y_axis_long] = dict(title=axis_names[i],
                                           range=[axis_class_list[i].y_range_min,
                                                  axis_class_list[i].y_range_max],
                                           dtick=axis_class_list[i].y_dtick,
                                           titlefont=dict(
                                               color=axis_colors[i]
                                           ),
                                           tickfont=dict(
                                               color=axis_colors[i]
                                           ),
                                           anchor="free" if i > 1 else 'x',
                                           overlaying="y",
                                           side="right",
                                           position=right_y_position + offset * (i-1))

    return yaxis_dict, right_y_position


# ## Column Ordering 

# In[313]:


def col_order(df):
    #level 0
    full_cols = df.columns.levels[:-1][0].tolist()
    cols0_main =  ['quarter_end_dates','month_date','week_date','Revenue_yoy_change','Revenue_accel','Revenue_2Ychange','Revenue_2Yaccel']
    cols0_end = ['quarter','actual_flag']
    cols0_other = [x for x in full_cols if x not in cols0_main and x not in cols0_end]
    
    avg_cols_gt = [x for x in cols0_other if 'Google Trends' in x and 'Avg_yoy' in x ]
    sum_cols_gt = [x for x in cols0_other if 'Google Trends' in x and 'Sum_yoy' in x ]
    twoY_gt = [x for x in cols0_other if 'Google Trends' in x and '2Ychange' in x ]
    avg_cols_gt.sort(key = lambda x: -len(x))
    sum_cols_gt.sort(key = lambda x: -len(x))
    twoY_gt.sort(key = lambda x: -len(x))
    
    
    avg_cols_other = [x for x in cols0_other if 'Avg_yoy' in x and x not in avg_cols_gt]
    sum_cols_other = [x for x in cols0_other if 'Sum_yoy_change' in x and x not in sum_cols_gt]
    twoY_other = [x for x in cols0_other if ('Avg_2Ychange' in x or 'Sum_2Ychange'in x )and x not in twoY_gt]
    avg_cols_other.sort(key = lambda x: (x.split('_')[0],-len(x)))
    sum_cols_other.sort(key = lambda x: (x.split('_')[0],-len(x)))
    twoY_other.sort(key = lambda x: (x.split('_')[0],-len(x)))
    
    
    avg_cols_gt.extend(avg_cols_other)
    sum_cols_gt.extend(sum_cols_other)
    twoY_gt.extend(twoY_other)
    avg_cols_gt.extend(sum_cols_gt)
    avg_cols_gt.extend(twoY_gt)
    
    accel_cols = [x for x in cols0_other if 'accel' in x]
    
    avg_cols_gt.extend(accel_cols)
    
    cols0_main.extend(avg_cols_gt)
    cols0_main.extend(cols0_end)
    

    df = df.reindex(cols0_main, axis = 1, level = 0) 
    return df 
    
    
def combine_cols(quart,month):
    
    #quarterly
    cols = list(quart.columns.levels[:-1][0])
    cols = [re.sub(r'quarterly_yoy_change',r'QM_yoy_WeightedAvg_change',x) for x in cols]
    cols = [re.sub(r'weekly_yoy_change',r'weekly_yoy_Avg_change',x) for x in cols]
    cols = [re.sub(r'daily_yoy_change',r'daily_yoy_Avg_change',x) for x in cols]
    quart= quart.rename(columns={key:value for key, value in zip(list(quart.columns.levels[:-1][0]), cols)})
    #monthly
    cols2 = list(month.columns.levels[:-1][0])
    cols2 = [re.sub(r'monthly_yoy_change',r'QM_yoy_WeightedAvg_change',x) for x in cols2]
    cols2 = [re.sub(r'monthly_aligned_yoy_change',r'yoy_Avg_change',x) for x in cols2]
    month = month.rename(columns={key:value for key, value in zip(list(month.columns.levels[:-1][0]), cols2)})
    
    return quart, month


# ## Axis Alignment 

# In[314]:


def align_yaxis(axes): 
    y_lims = np.array([ax.get_ylim() for ax in axes])

    # force 0 to appear on all axes, comment if don't need
    y_lims[:, 0] = y_lims[:, 0].clip(None, 0)
    y_lims[:, 1] = y_lims[:, 1].clip(0, None)

    # normalize all axes
    y_mags = (y_lims[:,1] - y_lims[:,0]).reshape(len(y_lims),1)
    y_lims_normalized = y_lims / y_mags

    # find combined range
    y_new_lims_normalized = np.array([np.min(y_lims_normalized), np.max(y_lims_normalized)])

    # denormalize combined range to get new axes
    new_lims = y_new_lims_normalized * y_mags
    for i, ax in enumerate(axes):
        ax.set_ylim(new_lims[i]) 


# # Data Stream Aggregation 
# 

# ## Quarterly Aggregation

# In[315]:


def quarterly_calcs(df, daily = False): 
    trend_names = df.select_dtypes(include = ['float64','int64']).columns.tolist()
    trend_names.sort()
    if daily == False:
        f = weekly_yoy
        f2 = weekly_2yoy
    else: 
        f = daily_yoy
        f2 = daily_2yoy
    

        
    names = [f'{trend}_quarterly_Avg_yoy_change' for trend in trend_names]
    quarterly_names =[f'{trend}_quarterly_Sum_yoy_change' for trend in trend_names]  
    twoY_names = [f'{trend}_quarterly_Avg_2Ychange' for trend in trend_names]
    accel_names =[re.sub(r'_yoy_change$',r'_accel', trend) for trend in names + quarterly_names]
    accel_names.extend([re.sub(r'_2Ychange$',r'_2Yaccel', trend) for trend in twoY_names])
    qts = ['quarter','quarter_end_dates']    
         


    #_yoy calcs
    df[names] = df[trend_names].agg(lambda x: f(pd.Series(x,index =df.index)))
    df[twoY_names] = df[trend_names].agg(lambda x: f2(pd.Series(x,index =df.index)))
    #yoy df 
    names.extend(twoY_names)
    names.extend(qts)
    trends = df[names]

    #quarter agg 
    quart = trends.groupby(['quarter','quarter_end_dates']).mean()

    #quarterly yoy change 
    quart[quarterly_names] = df[trend_names].agg(lambda x: quarterly_yoy(df[[x.name,'quarter','quarter_end_dates']])[x.name])
    #accels 
    quart[accel_names] =   quart.select_dtypes(include = ['float64','int64']).agg(lambda x: pd.Series(x, index = quart.index).pct_change(1)*100)
    
    
    
    return quart


# In[316]:


def quarterly_df(ticker,drop_dates =[],to_display = None,insta = True): 
    df_dic = get_dfs(ticker,insta = insta)
    quart_lst = []
    wk_df, day_df = df_dic['weekly_df'], df_dic['daily_df']
    quart_w, quart_d = quarterly_calcs(wk_df), quarterly_calcs(day_df, daily = True)
    quart_lst.append(quart_w)
    quart_lst.append(quart_d)
    #revenue
    rev_df = df_dic['revenue_df']
    rev_df['Revenue_2Ychange'] = rev_df['Total Revenue'].pct_change(8) * 100
    rev_df['Total Revenue'] = rev_df['Total Revenue'].pct_change(4)
    rev_df['Total Revenue'] = rev_df['Total Revenue'] * 100
    rev_df = rev_df.rename(columns ={'Total Revenue': 'Revenue_yoy_change'})
    rev_df['Revenue_accel'] = rev_df['Revenue_yoy_change'].pct_change(1) *100
    rev_df['Revenue_2Yaccel'] = rev_df['Revenue_2Ychange'].pct_change(1) *100
    rev_df = rev_df.reset_index()
    rev_df = rev_df.set_index(['quarter','quarter_end_dates'])
    quart_lst += [rev_df]
    
    df_final = pd.concat(quart_lst, axis = 1)
    
    #FQ col 
    act = df_final.loc[df_final.actual_flag == 'A',:].index
    est = df_final.loc[df_final.actual_flag == 'E',:].index
    fq_act = [f'FQ{i}' for i in range(-len(act)+1,1)]
    fq_est = [f'FQ{i}' for i in range(1,len(est)+1)]
    fq_act.extend(fq_est)
    df_final['FQ'] = fq_act
    
    #date dropping 
    if len(drop_dates) != 0 and len(re.findall(r'^\d.$', drop_dates[0])) == 0:
        
        
        #if in YQ format 
        if len(re.findall(r'^\d.*Q.*$',drop_dates[0])) != 0:
            df_final.drop(pd.PeriodIndex(drop_dates,freq= df_final.reset_index().quarter.dt.freq), level=0)
        
        #if in FQ format 
                
        elif len(re.findall(r'^FQ.*$',drop_dates[0])) != 0:
                df_final = df_final.drop(df_final.loc[df_final.FQ.isin(drop_dates)].index)
        #quarter_end_date format
        else:
            drop_dates = [datetime.strptime(x,'%Y-%m-%d') for x in drop_dates]
            df_final = df_final.drop(drop_dates,level = 1)
    
    
    
    #order cols 
    cols = df_final.columns.tolist()
    main_cols= [ 'FQ','ticker', 'Revenue_yoy_change','Revenue_accel','Revenue_2Ychange', 'Revenue_2Yaccel','actual_flag']
    cols = [x for x in cols if x not in main_cols]
    #order data stream cols 
    main_cols.extend(cols)
    df_final = df_final.reindex(main_cols,axis = 1)
    df_final = df_final.dropna(thresh = 8)
    
    #display
    if to_display != None:
        df_final = quart_num(df_final,to_display = to_display)
    return df_final
    
    
    


# ## Monthly Aggregation

# In[317]:


def monthly_calcs(df, daily = False): 
    from pandas.tseries.offsets import MonthEnd
    trend_names = df.select_dtypes(include = ['float64','int64']).columns.tolist()
    trend_names.sort()
    if daily == False:
        f = weekly_yoy
    else: 
        f = daily_yoy
    
    freq = df.quarter.dt.freq
    df['date'] = pd.to_datetime(df.index.strftime('%Y-%m'),format="%Y-%m") + MonthEnd()

        
    names = [f'{trend}_monthly_Avg_yoy_change' for trend in trend_names]
    monthly_names =[f'{trend}_monthly_Sum_yoy_change' for trend in trend_names]
    accel_names =[re.sub(r'_yoy_change$',r'_accel', trend) for trend in names + monthly_names]
    qts = ['quarter','quarter_end_dates','date']    
         
    

    #_yoy calcs
    df[names] = df[trend_names].agg(lambda x: f(pd.Series(x,index =df.index)))
    names.extend(qts)
    trends = df[names]
    
    trends = trends.reset_index(drop = True)
    trends = trends.set_index('date')
    trends = trends.groupby(['quarter','quarter_end_dates','date']).mean()
    names.extend(monthly_names)
    monts = df[trend_names].agg(lambda x: monthly_yoy(pd.Series(x,index =df.index)))
    trends = trends.reset_index()
    trends = trends.set_index('date')
    trends[monthly_names] = monts
    trends[accel_names] =  trends.select_dtypes(include = ['float64','int64']).agg(lambda x: pd.Series(x, index = trends.index).pct_change(1)*100)
    trends = trends.reset_index()
    trends = trends.set_index(['quarter','quarter_end_dates','date'])
    
    
    
    return trends


# In[318]:


def monthly_df(ticker, to_display = None,insta = True):
    df_dic = get_dfs(ticker, insta = insta)
    #do all monthly calcs 
    wk_df, day_df,rev_df = df_dic['weekly_df'], df_dic['daily_df'], df_dic['revenue_df']
    month_w, month_d = monthly_calcs(wk_df), monthly_calcs(day_df,daily= True)
    
    #revenue 
    rev_df['Revenue_2Ychange'] = rev_df['Total Revenue'].pct_change(8) * 100
    rev_df['Total Revenue'] = rev_df['Total Revenue'].pct_change(4)
    rev_df['Total Revenue'] = rev_df['Total Revenue'] * 100
    rev_df = rev_df.rename(columns ={'Total Revenue': 'Revenue_yoy_change'})
    rev_df['Revenue_accel'] = rev_df['Revenue_yoy_change'].pct_change(1) *100
    rev_df['Revenue_2Yaccel'] = rev_df['Revenue_2Ychange'].pct_change(1) *100
    #join
    df_final = pd.merge(month_w, month_d, on = ['quarter','quarter_end_dates','date'], how = 'outer')
    df_final = df_final.reset_index()
    df_final = pd.merge(rev_df,df_final, on = ['quarter','quarter_end_dates'],how = 'outer')
    #build FQ 
    act = df_final.loc[df_final.actual_flag == 'A']
    est = df_final.loc[df_final.actual_flag == 'E']
    q_act = list(set(act.quarter)) 
    q_est = list(set(est.quarter))
    q_act.sort()
    q_est.sort()
    
    map_act = {q_act:f'FQ{x}' for q_act,x in zip(q_act,range(-len(q_act)+1,1))}
    map_est = {q_est:f'FQ{x}' for q_est,x in zip(q_est,range(1, len(q_est)+1))}
    map_act.update(map_est)
    
    df_final['FQ'] =df_final.quarter.replace(map_act)
    
    #to display 
    if to_display != None:
        df_final = quart_num(df_final,to_display = to_display,quart = False)

    #order cols 
    
    cols = df_final.columns.tolist()
    main_cols= [ 'FQ','ticker', 'Revenue_yoy_change','Revenue_accel','Revenue_2Ychange','Revenue_2Yaccel', 'actual_flag']
    cols = [x for x in cols if x not in main_cols]
    #order data stream cols 
    main_cols.extend(cols)
    #set index 
    df_final = df_final.set_index(['quarter','quarter_end_dates','date'])
    df_final = df_final.reindex(main_cols,axis = 1)
    try: 
        df_final = df_final.drop(['quarter','quarter_end_dates','date'],axis =1 )
    except: 
        pass
    df_final = df_final.dropna(thresh = 8)
    return df_final
    
    


# ## Weekly Aggregation 

# In[319]:


def weekly_calcs(df, ind = None, daily = False): 
    trend_names = df.select_dtypes(include = ['float64','int64']).columns.tolist()
    trend_names.sort()
    
    names = [f'{trend}_weekly_Avg_yoy_change' for trend in trend_names]
    accel_names =[f'{trend}_weekly_Avg_accel' for trend in trend_names]
    
    qt = ['quarter','quarter_end_dates']
    if daily == False:
        f = weekly_yoy
        df[names] = df[trend_names].agg(lambda x: f(pd.Series(x, index =df.index)))
        df[accel_names] = df[names].agg(lambda x: pd.Series(x, index = df.index).pct_change(1)*100)
        trends = df.drop(trend_names, axis = 1)
    else: 
        f = weekly_ser_daily
        trends = df[trend_names].agg(lambda x: f(pd.Series(x, index = df.index),ind = ind)[x.name])
        trends.columns = names 
        trends[accel_names] = trends[names].agg(lambda x: pd.Series(x, index = trends.index).pct_change(1)*100)
    

    return trends 


# In[320]:


def weekly_df(ticker,to_display = None,insta = True): 
    #get dict 
    df_dic = get_dfs(ticker, insta = insta)
    #get dfs 
    wk_df, day_df,rev_df = df_dic['weekly_df'], df_dic['daily_df'], df_dic['revenue_df']
    #calcs 
    week_w, week_d = weekly_calcs(wk_df), weekly_calcs(day_df, ind = wk_df.index, daily = True)
    #merge
    df_final = pd.merge(week_w,week_d, on = 'date', how = 'outer') 
    df_final = df_final.reset_index()
    #rev df 
    rev_df['Revenue_2Ychange'] = rev_df['Total Revenue'].pct_change(8) * 100
    rev_df['Total Revenue'] = rev_df['Total Revenue'].pct_change(4)
    rev_df['Total Revenue'] = rev_df['Total Revenue'] * 100
    rev_df = rev_df.rename(columns ={'Total Revenue': 'Revenue_yoy_change'})
    rev_df['Revenue_accel'] = rev_df['Revenue_yoy_change'].pct_change(1) *100
    rev_df['Revenue_2Yaccel'] = rev_df['Revenue_2Ychange'].pct_change(1) *100
    #merge 
    df_final = pd.merge(rev_df, df_final, on = ['quarter','quarter_end_dates'],how = 'outer')
    #build FQ
    act = df_final.loc[df_final.actual_flag == 'A']
    est = df_final.loc[df_final.actual_flag == 'E']
    q_act = list(set(act.quarter)) 
    q_est = list(set(est.quarter))
    q_act.sort()
    q_est.sort()
    
    map_act = {q_act:f'FQ{x}' for q_act,x in zip(q_act,range(-len(q_act)+1,1))}
    map_est = {q_est:f'FQ{x}' for q_est,x in zip(q_est,range(1, len(q_est)+1))}
    map_act.update(map_est)
    
    df_final['FQ'] =df_final.quarter.replace(map_act)
    #to display 
    if to_display != None:
        df_final = quart_num(df_final,to_display = to_display,quart = False)
    
    cols = df_final.columns.tolist()
    main_cols= [ 'FQ','ticker', 'Revenue_yoy_change','Revenue_accel','Revenue_2Ychange', 'Revenue_2Yaccel','actual_flag']
    cols = [x for x in cols if x not in main_cols]
    #order data stream cols 
    main_cols.extend(cols)
    
    #set index 
    df_final = df_final.set_index(['quarter','quarter_end_dates','date'])
    df_final = df_final.reindex(main_cols,axis = 1)
    df_final = df_final.drop(['quarter','quarter_end_dates','date'],axis =1 )
    try: 
        df_final = df_final.drop(['quarter','quarter_end_dates','date'],axis =1 )
    except: 
        pass
    df_final = df_final.dropna(thresh = 8)
    return df_final
    
    


# # Screeners 

# ## Quarterly Screener

# In[321]:


def screen_quarterly(ticker_list,to_display = None, missing = False, flip = False,insta = True, drop_dates = []):
    '''Stats calculations not Affected by Display'''
    from collections import defaultdict 
    df_dict = defaultdict(list)
    df_lst = []
    missing = []
    
    for ticker in ticker_list:
        try:
            #build dfs
            df = quarterly_df(ticker, drop_dates = drop_dates,insta =insta)
            df = stats_df(df,drop_dates = drop_dates)
            #to display 
            if to_display != None:    
                df = quart_num(df,to_display = to_display)
            #drop cols 
            df = df.drop('ticker',axis = 1)
            #unstack and build 
            df = df.unstack().to_frame().T
            #set index 
            df.index = [ticker]
            #append
            df_lst.append(df)
        except Exception as e:
            print(ticker,e)
            missing.append(ticker)
            continue
        df_dict[len(df.columns)] = df.columns
        
    
    if len(missing) != 0 and missing == True: 
        print('Missing Tickers:')
        display(missing)
    max_df = max(df_dict.keys())
    df_final = pd.concat(df_lst)[df_dict[max_df].tolist()]
    #order cols
    #level 0
    cols = [x for x in df_dict[max_df].levels[:-1][0]]
    cols_main =  ['quarter_end_dates','Revenue_yoy_change','Revenue_accel','Revenue_2Ychange','Revenue_2Yaccel']
    cols_end = ['quarter','actual_flag']
    cols_other = [x for x in cols if x not in cols_main and x not in cols_end]
    cols_main.extend(cols_other)
    cols_main.extend(cols_end)
    df_final = df_final.reindex(cols_main,axis =1,level = 0)
    if flip == False:
        df_final = df_final.dropna(axis = 1,how ='all')
        return df_final
    else: 
        return df_final.stack().T
            


# ## Monthly Screener 

# In[322]:


def screen_monthly(ticker_list,to_display = None, missing = False, flip = False,insta = True):
    #Get dict of dfs 
    df_lst = []
    missing = []
    df_dict = defaultdict(list)
    for ticker in ticker_list:
        try:
            #build df 
            df = monthly_df(ticker,to_display = to_display,insta = insta)
            df = df.reset_index()
            #rename month dates
            df= df.rename(columns ={'date':'month_date'})
            #build index 
            ser_mo = df.quarter.value_counts()
            ser_mo = ser_mo.sort_index()
            ser_mo.index = df.FQ.unique()
            lst = []
            for q in ser_mo.index:
                lst += [x for x in range(1,ser_mo[q]+1)]
            index = [f'{x}_{m}' for x,m in zip(df.FQ.tolist(), lst)]
            #set index 
            df.index = index 
            #drop cols 
            df = df.drop(['FQ','ticker'],axis = 1)
            #unstack 
            df = df.unstack().to_frame().T
            #set index and append
            df.index = [ticker]
            df_lst.append(df)
        except Exception as e: 
            print(ticker,e)
            missing.append(ticker)
            continue
        df_dict[len(df.columns)] = df.columns 

    if len(missing) != 0 and missing == True: 
        display(missing)
    max_df = max(df_dict.keys())
    df_final = pd.concat(df_lst)[df_dict[max_df]]
    #order columns
    #level 0 
    cols = [x for x in df_dict[max_df].levels[:-1][0]]
    cols_main =  ['quarter_end_dates','month_date','Revenue_yoy_change','Revenue_accel','Revenue_2Ychange','Revenue_2Yaccel']
    cols_end = ['quarter','actual_flag']
    cols_other = [x for x in cols if x not in cols_main and x not in cols_end]
    cols_main.extend(cols_other)
    cols_main.extend(cols_end)
    
    
    df_final = df_final.reindex(cols_main,axis = 1,level =0)
    if flip == False:
        df_final = df_final.dropna(axis = 1,how = 'all')
        return df_final
    else: 
        return df_final.stack().T
    
            
    


# ## Weekly Screen 

# In[323]:


def display_wks(df,wk_display = None): 
    dt_lst =[]
    n = wk_display
    for q in df.quarter.unique(): 
        dates = df.loc[df.quarter == q].date.tolist()
        if len(dates) <= n: 
            dt_lst.extend(dates)
        else: 
            dt_lst.extend(dates[-n:])
    return df.loc[df.date.isin(dt_lst),:]


# In[324]:


def screen_weekly(ticker_list,to_display = None, wk_display = None, missing = False, flip = False,insta = True): 
    #Get dict of dfs 
    df_lst = []
    missing = []
    df_dict = defaultdict(list)
    for ticker in ticker_list:
        try:
            #build df 
            df = weekly_df(ticker,to_display = to_display,insta = insta)
            df = df.reset_index()
            #display desired number of weeks 
            if wk_display != None: 
                df = display_wks(df,wk_display = wk_display)
            #build index 
            index =  [f'{q}_{w}' for q,w in zip(df.FQ.tolist(),df.date.dt.strftime('%m/%d').tolist())]
            #rename week dates
            df= df.rename(columns ={'date':'week_date'})
            #drop 
            df = df.drop(['FQ','ticker'],axis = 1)
            #set_index 
            df.index = index 
            #unstack and build 
            df = df.unstack().to_frame().T
            df.index = [ticker]
            df_lst.append(df)
        except Exception as e: 
            print(ticker,e)
            missing.append(ticker)
            continue
        df_dict[len(df.columns)] = df.columns 
    
    if len(missing) != 0 and missing == True: 
        display(missing)
    max_df = max(df_dict.keys())
    df_final = pd.concat(df_lst)[df_dict[max_df]]
    #order columns
    #level 0 
    cols = [x for x in df_dict[max_df].levels[:-1][0]]
    cols_main =  ['quarter_end_dates','week_date','Revenue_yoy_change','Revenue_accel','Revenue_2Ychange','Revenue_2Yaccel']
    cols_end = ['quarter','actual_flag']
    cols_other = [x for x in cols if x not in cols_main and x not in cols_end]
    cols_main.extend(cols_other)
    cols_main.extend(cols_end)
    
    
    df_final = df_final.reindex(cols_main,axis = 1,level =0)
    if flip == False:
        df_final = df_final.dropna(axis = 1,how = 'all')
        return df_final
    else: 
        return df_final.stack().T
    
    


# ## Combining Screeners 

# In[325]:


def combine_screen(ticker_list, drop_dates = [], to_display = None,wk_display = None, flip = False,insta = True,earn = False):

#     missing = []


#     for ticker in ticker_list:
#         try:
#             #go through tickers individually 
#             quart = screen_quarterly([ticker],to_display = 12,drop_dates = drop_dates,insta = insta)
#             month = screen_monthly([ticker],to_display = 12,insta = insta)
#             #rename cols
#             quart,month = combine_cols(quart,month)
#             #join 
#             df_main = pd.concat([quart,month],axis=0, ignore_index = True)
#             df_main = df_main.fillna(method = 'bfill').drop(1,axis =0)
#             df_main = df_main.set_index(pd.Index([ticker]))
#             df_lst.append(df_main)
#         except Exception as e:
#             print(ticker,e)
#             missing.append(ticker)
#             continue
        

    quart = screen_quarterly(ticker_list,to_display = to_display, missing = True, insta = insta,drop_dates = drop_dates)
    month = screen_monthly(ticker_list,to_display = to_display, insta = insta)
    week = screen_weekly(ticker_list, to_display = to_display, wk_display = wk_display,insta = insta) 
    #drop cols
    month_full = month.drop(['quarter_end_dates','Revenue_yoy_change','Revenue_accel','Revenue_2Ychange','Revenue_2Yaccel','quarter','actual_flag'],axis = 1)
    week_full = week.drop(['quarter_end_dates','Revenue_yoy_change','Revenue_accel','Revenue_2Ychange','Revenue_2Yaccel','quarter','actual_flag'],axis = 1)
    df_final = pd.concat([quart,month_full,week_full],axis = 1)
    
    #order cols
    df_final = col_order(df_final)

    
    if flip == False:
        if earn == True: 
            return df_final.dropna(axis = 1,how ='all'), quart, week
        else: 
            return df_final.dropna(axis = 1,how ='all')
    
    else: 
        return df_final.stack().T
    
        
     
        

            
            
            

    


# ## Faster running for ticker chunks

# In[326]:


def block_run(ticker_lst,n,chunks,insta = True):
    
    tasks = []
    def screen_block(ticker_lst, n): 
        try: 
            df = combine_screen(ticker_lst,insta = insta)
            df.to_pickle(f'chunk_screen_{n}.pkl')
        except Exception as e: 
            print(e)
    for n, ticker in enumerate(ticker_lst[chunk:]): 
        task = delayed(screen_block)([ticker],n)
        tasks.append(task)
    compute(*tasks)
    
    
    from pathlib import Path
    from tqdm import tqdm_notebook as tqdm 
    import os
    path = Path(os.getcwd)
    chunk_files = [f for f in path.iterdir() if name.split('_')[0]=='chunk']
    
    df = pd.DataFrame()
    for f in tqdm(chunk_files): 
        _ = pd.read_pickle(f)
        df= df.append(_)
    return df


# # Full Plotting Function 
# 
# Functionality: 
# 
# 1) display the number of quarters desired without affecting the calculations (all data points are internally used)
# 
# 2) passing drop dates will affect the calculations and allow you to remove desired data points from generated plots 
# 
#     2a) in the case drop_dates is passed a number (format: ['number']) this will affect the calculations however it will not change the display quarters 
# 
# 3) decompose will allow you to view the behavior of the trends alongside revenue over time thereby uncovering useful trends 
# 
# 4) resid will allow you to view the histograms of the noise components in each trend and hence give you an idea of best trends as well as any missing information that was missed in the observed data
# 
# 

# In[327]:


def plot_main(ticker_lst,line = True,decompose = False,resid = False,matrix = False,insta = True, missing = False,df = None,df_weekly = None,df_monthly = None, to_display = None, drop_dates=[]):
    #df = screen_quarterly(ticker_lst, to_display= to_display,drop_dates = drop_dates,flip= False,missing = missing)
    if df is not None: 
        df = df
        if df_weekly is not None: 
            df_weekly = df_weekly
            if df_monthly is not None:
                df_monthly = df_monthly
    else: 
        df = screen_quarterly(ticker_list = ticker_lst, to_display =to_display,insta = insta, drop_dates= drop_dates,flip = False,missing = missing )
    
    for ticker in ticker_lst: 
        try:
            df_t = df.loc[ticker].to_frame().T.stack()
            df_t = df_t.reset_index(level = 0,drop = True)
            df_t['quarter_end_dates'] = pd.to_datetime(df_t['quarter_end_dates'])
            #monthly plotting function
            if df_weekly is not None and df_monthly is not None: 
                df_w = df_weekly.loc[ticker].to_frame().T.stack()
                df_w = df_w.reset_index(level = 0,drop = True)
                df_w = df_w.reset_index()
                df_m = df_monthly.loc[ticker].to_frame().T.stack()
                df_m = df_m.reset_index(level = 0,drop = True)
                df_m = df_m.reset_index()
                scatt(df_t, ticker,drop_dates = drop_dates, decompose = decompose, resid = resid,matrix =matrix,line = line,df_w= df_w, df_m = df_m)
                
            elif df_weekly is not None and df_monthly is None: 
                df_w = df_weekly.loc[ticker].to_frame().T.stack()
                df_w = df_w.reset_index(level = 0,drop = True)
                df_w = df_w.reset_index()
                scatt(df_t, ticker,drop_dates = drop_dates, decompose = decompose, resid = resid,matrix =matrix,line = line,df_w= df_w, df_m = None)
            elif df_monthly is not None and df_weekly is None: 
                df_m = df_monthly.loc[ticker].to_frame().T.stack()
                df_m = df_m.reset_index(level = 0,drop = True)
                df_m = df_m.reset_index()
                scatt(df_t, ticker,drop_dates = drop_dates, decompose = decompose, resid = resid,matrix =matrix,line = line,df_w= None, df_m = df_m)
            else: 
                scatt(df_t, ticker,drop_dates = drop_dates, decompose = decompose, resid = resid,matrix =matrix,line = line,df_w = None, df_m = None)
        except Exception as e:
            print(e,ticker)
            continue
    


                


# In[328]:


def scatt(df,ticker,drop_dates = [],line = True,decompose = False,resid = False,matrix = False,df_w = None,df_m =None):
    #set up trend_dict
    main_cols = df.columns.tolist()
    cols = [x for x in main_cols if 'accel' not in x and '2Yaccel' not in x]
    accel_cols = [x for x in main_cols if 'accel' in x or '2Yaccel' in x]
    cols_dict = defaultdict(list)
    accel_dict = defaultdict(list)
    cols_dict['gtrends'] = [x for x in cols if re.search(r'^.*Global.*$',x)]
    accel_dict['gtrends'] = [x for x in accel_cols if re.search(r'^.*Global.*$',x)]
    cols_dict['gtrends_us'] = [x for x in cols if re.search(r'^.*_US.*$',x)]
    accel_dict['gtrends_us'] = [x for x in accel_cols if re.search(r'^.*_US.*$',x)]
    
    cols_dict['alex_pg'] = [x for x in cols if re.search(r'^.*Page.*$',x)]
    accel_dict['alex_pg'] = [x for x in accel_cols if re.search(r'^.*Page.*$',x)]
    cols_dict['alex_rch'] = [x for x in cols if re.search(r'^.*Reach.*$',x)]
    accel_dict['alex_rch'] =[x for x in accel_cols if re.search(r'^.*Reach.*$',x)]
    
    cols_dict['twit'] = [x for x in cols if re.search(r'^Twitter.*$',x)]
    accel_dict['twit'] = [x for x in accel_cols if re.search(r'^Twitter.*$',x)]
    
    cols_dict['inst_comm'] = [x for x in cols if re.search(r'^Instagram COMMENTS.*$',x)]
    accel_dict['inst_comm'] = [x for x in accel_cols if re.search(r'^Instagram COMMENTS.*$',x) ]
    cols_dict['inst_likes'] = [x for x in cols if re.search(r'^Instagram LIKES.*$',x)]
    accel_dict['inst_likes'] = [x for x in accel_cols if re.search(r'^Instagram LIKES.*$',x)]
    cols_dict['inst_posts'] = [x for x in cols if re.search(r'^Instagram POSTS.*$',x)]
    accel_dict['inst_posts'] = [x for x in accel_cols if re.search(r'^Instagram POSTS.*$',x)]
    #weekly 
    wk_dict = defaultdict(list)
    if df_w is not None: 
        df_w = df_w.replace(np.inf, df_w.median())
        df_w = df_w.reset_index()
        df_w['week_date'] = pd.to_datetime(df_w['week_date'])
        df_w = df_w.set_index('week_date')
        #dicts 
        wk_cols = df_w.columns
        wk_cols = [x for x in wk_cols if 'accel' not in x]
        wk_dict['gtrends'] =  [x for x in wk_cols if re.search(r'^.*Global.*$',x)]
        wk_dict['gtrends_us']=[x for x in wk_cols if re.search(r'^.*_US.*$',x)]
        wk_dict['alex_pg'] = [x for x in wk_cols if re.search(r'^.*Page.*$',x)]
        wk_dict['alex_rch'] = [x for x in wk_cols if re.search(r'^.*Reach.*$',x)]
        wk_dict['twit'] =[x for x in wk_cols if re.search(r'^Twitter.*$',x)]
        wk_dict['insta_comm'] = [x for x in wk_cols if re.search(r'^Instagram COMMENTS.*$',x)]
        wk_dict['insta_likes'] = [x for x in wk_cols if re.search(r'^Instagram LIKES.*$',x)]
        wk_dict['insta_posts'] = [x for x in wk_cols if re.search(r'^Instagram POSTS.*$',x)]
    df = df.replace(np.inf,df.median())
    fqs_t = [x for x in df.index if re.search(r'^FQ(.\d|.|.\d\d)$',x)]
    #time series
    df_ts = df.loc[fqs_t,:]
    df_ts = df_ts.reset_index()
    df_ts = df_ts.set_index('quarter_end_dates')
    df_ts.index = pd.DatetimeIndex(df_ts.index,freq = 'infer')
    #drop dates
    if len(drop_dates) != 0 and len(re.findall(r'^\d.$', drop_dates[0])) == 0 :

        try:
            #if in YQ format 
            if len(re.findall(r'^\d.*Q.*$',drop_dates[0])) != 0: 
                df = df.drop(df.loc[df.quarter.isin(pd.PeriodIndex(drop_dates,freq=df.quarter.dt.freq))].index)


            #if in m/Y format or Y/m
            elif len(re.findall(r'^\d{2}/\d{4}$',drop_dates[0])) != 0 or len(re.findall(r'^\d{4}/\d{2}$',drop_dates[0])) !=0:
                df = df.drop(df.loc[(df.quarter_end_dates.dt.strftime('%m/%Y').isin(drop_dates))|(df.quarter_end_dates.strftime('%Y/%m').isin(drop_dates)) ,:].index)

            #FQ format 
            elif len(re.findall(r'^FQ.*$',drop_dates[0])) != 0:
                df = df.drop(drop_dates)
            #quarter_end_date format
            else:
                drop_dates = [datetime.strptime(x,'%Y-%m-%d') for x in drop_dates]
                df = df.drop(df.loc[df.quarter_end_dates.dt.isin(drop_dates),:].index)
        except Exception as e:
            print('Error Whie Dropping The Dates:',e)
            pass 
        
    #set up df for plotting 
    df_stat = df.loc[['R^2_including_FQ0','Adj_R^2_including_FQ0','slope_including_FQ0','intercept_including_FQ0',
         'PValue_including_FQ0','FQ1_Surp_including_FQ0','FQ1_Forecast_including_FQ0'],:]
    fqs =[x for x in df.index if re.search(r'^FQ(.\d|.|.\d\d)$',x)]

    df_plts = df.loc[fqs,:]

    print('\n')
    comp = f'<h1>{ticker} Plots:</h1>'
    display(HTML(comp))
    print('\n')
    df_dis = df_ts.copy()
    df_dis = df_dis.reset_index()
    df_dis = df_dis.set_index('quarter_end_dates')
    df_wid = qgrid.show_grid(df_dis.drop('quarter',axis =1),precision=2,grid_options = {'forceFitColumns': False, 'maxVisibleRows':30,
    'minVisibleRows': 8,'autoHeight': True, },show_toolbar = True)
    display(df_wid)
    #plotting 
    for key in cols_dict:
        if len(cols_dict[key]) == 0: 
            continue
        try:
            cols_dict[key].sort(key = lambda x: (x.split('_')[-1][0],x.split('_')[1],x.split('_')[2]),reverse = True)
            accel_dict[key].sort(key = lambda x: (x.split('_')[-1][0],x.split('_')[1],x.split('_')[2]),reverse = True)
            df_act = df.loc[df.actual_flag == 'A',cols_dict[key]]
            df_stats = df_stat.loc[:,cols_dict[key]]
            cols_dict[key].append('Revenue_yoy_change')
            cols_dict[key].append('Revenue_2Ychange')
            accel_dict[key].append('Revenue_accel')
            accel_dict[key].append('Revenue_2Yaccel')
            wk_dict[key].append('Revenue_yoy_change')
            wk_dict[key].append('Revenue_2Ychange')
            df_plt = df_plts[cols_dict[key]]
            df_plt = df_plt.dropna()
            df_accel = df_plts[accel_dict[key]]
            if len(df_plt) == 0: 
                continue 
            
            #hit rate calcs
            hit_rates = []
            rev_accel = df_accel['Revenue_accel']
            rev2_accel = df_accel['Revenue_2Yaccel'] 
            
            accel1 = df_accel[accel_dict[key][0]] *rev_accel
            accel1 = accel1.dropna()
            hit1 = 100. * (accel1 >= 0).sum() / len(accel1.index)
            
            accel2 = df_accel[accel_dict[key][1]] *rev_accel
            accel2 = accel2.dropna()
            hit2 = 100. * (accel2 >= 0).sum() / len(accel2.index)
            
            accel3 = df_accel[accel_dict[key][2]] *rev2_accel
            accel3 = accel3.dropna()
            hit3 = 100. * (accel3 >= 0).sum() / len(accel3.index)
            
            hit_rates.extend([hit1,hit2,hit3])
            
           
            
            #stats
            slopes = df_stats.loc['slope_including_FQ0',:].tolist()
            slope,slope2,slope3 = slopes[0],slopes[1],slopes[2]
            p_vals = df_stats.loc['PValue_including_FQ0',:].tolist()
            p_val,p_val2,p_val3 = p_vals[0],p_vals[1],p_vals[2]
            intercepts = df_stats.loc['intercept_including_FQ0',:].tolist()
            intercept, intercept2,intercept3 = intercepts[0],intercepts[1],intercepts[2]
            forcs = df_stats.loc['FQ1_Forecast_including_FQ0',:].tolist()
            forc,forc2,forc3 = forcs[0],forcs[1],forcs[2]
            
            surps = df_stats.loc['FQ1_Surp_including_FQ0',:].tolist()
            surp,surp2,surp3 = surps[0],surps[1],surps[2]

            r_sqs = df_stats.loc['R^2_including_FQ0',:].tolist()
            r_sq,r_sq2,r_sq3 = r_sqs[0],r_sqs[1],r_sqs[2]

            adj_rs = df_stats.loc['Adj_R^2_including_FQ0',:].tolist()
            adj_r, adj_r2,adj_r3 = adj_rs[0],adj_rs[1],adj_rs[2]

            x = df_act[cols_dict[key][0]]
            x2 = df_act[cols_dict[key][1]]
            x3 = df_act[cols_dict[key][2]]
            
            est_1 = df_plts.loc[df_plts.actual_flag == 'E',cols_dict[key][0]].tolist()[0]
            est_2 = df_plts.loc[df_plts.actual_flag == 'E',cols_dict[key][1]].tolist()[0]
            est_3 = df_plts.loc[df_plts.actual_flag == 'E',cols_dict[key][2]].tolist()[0]
            rev_est = df_plts.loc[df_plts.actual_flag == 'E',cols_dict[key][3]].tolist()[0]


            #set up plots 
            fig = plt.figure()
            gs = fig.add_gridspec(4,4)
            gs.update(wspace = 0.3,hspace = 0.8)
            ax1 = fig.add_subplot(gs[:2, :2])
            ax2 = fig.add_subplot(gs[:2, 2:])
            ax3 = fig.add_subplot(gs[2:4, 1:3])
            #scatter plot 
            #first plot
            df_plt.plot(x = cols_dict[key][0], y = 'Revenue_yoy_change',kind= 'scatter',ax = ax1,s = 150,color= 'black',marker = '+',figsize = (18,20))
            for k,v in df_plt.iloc[:,[0,3]].iterrows(): 
                if re.search(r'^FQ(1|2|3)$', k):
                    ax1.annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
                    family='sans-serif', fontsize=16, ha = 'left', va = 'top', color='blue')
                else: 
                    ax1.annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
                    family='sans-serif', fontsize=12, ha = 'left', va = 'top', color='black')
         
            #plotting trend lines 

            ax1.plot(x,slope*x + intercept,linewidth =5,color = 'black')

            ax1.set_title(f'\n {ticker} \n \n Revenue vs. {cols_dict[key][0].split(r"_")[0]} \n {" ".join(cols_dict[key][0].split(r"_")[1:])} \n',fontsize = 19.5)
            
            ax1.text(-0.06,0.87,s =f'y = {slope:.3f} x + {intercept:.3f}',color = 'black',size = 20, ha='center', va='top',transform=fig.transFigure ) 
            ax1.text(-0.06,0.84,s = f'P_value = {p_val:.3f} \n \n R^2: {r_sq:.3f} \n \n Adj_R^2: {adj_r:.3f} \n \n FQ1_Trend_E = {est_1:.3f} \n\n FQ1_Rev_E = {rev_est:.3f} \n \n FQ1_Forecast = {forc:.3f} \n \n FQ1_Surp = {surp:.3f}'
                        ,color = 'black',size = 20, ha='center', va='top',transform=fig.transFigure)

            
            #second plot 
            df_plt.plot(x = cols_dict[key][1], y = 'Revenue_yoy_change',kind= 'scatter',ax = ax2,s = 150,color= 'red',marker = '+',figsize = (18,20))

            for k,v in df_plt.iloc[:,[1,3]].iterrows(): 
                if re.search(r'^FQ(1|2|3)$', k):
                    ax2.annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
                    family='sans-serif', fontsize=16, ha = 'left', va = 'top', color='blue')
                else:
                    ax2.annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
                    family='sans-serif', fontsize=12, ha = 'left', va = 'top', color='red')

            ax2.plot(x2,slope*x2 + intercept2,linewidth =5,color = 'red')

            ax2.set_title(f'\n {ticker} \n \n Revenue vs. {cols_dict[key][1].split(r"_")[0]} \n {" ".join(cols_dict[key][1].split(r"_")[1:])} \n',fontsize = 19.5)

            ax2.text(1.06,0.87,f'y = {slope2:.3f} x + {intercept2:.3f}',color = 'red',size = 20, ha = 'center',va = 'top',transform=fig.transFigure)
            ax2.text(1.06,0.84,f'P_value = {p_val2:.3f} \n \n R^2: {r_sq2:.3f} \n \n Adj_R^2: {adj_r2:.3f} \n \n FQ1_Trend_E = {est_2:.3f} \n\n FQ1_Rev_E = {rev_est:.3f}\n \n FQ1_Forecast = {forc2:.3f} \n \n FQ1_Surp = {surp2:.3f}',color = 'red',size = 20,ha = 'center',va = 'top',transform=fig.transFigure)
            
            #third plot
            df_plt.plot(x = cols_dict[key][2], y = 'Revenue_2Ychange',kind= 'scatter',ax = ax3,s = 150,color= 'green',marker = '+',figsize = (18,20))
            
            for k,v in df_plt.iloc[:,[2,4]].iterrows(): 
                if re.search(r'^FQ(1|2|3)$', k):
                    ax3.annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
                    family='sans-serif', fontsize=16, ha = 'left', va = 'top', color='blue')
                else:
                    ax3.annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
                    family='sans-serif', fontsize=12, ha = 'left', va = 'top', color='green')
            
            
            
            ax3.plot(x3,slope*x3 + intercept3,linewidth =5,color = 'green')

            ax3.set_title(f'\n {ticker} \n \n Revenue_2Y vs. {cols_dict[key][2].split(r"_")[0]} \n {" ".join(cols_dict[key][2].split(r"_")[1:])} \n',fontsize = 19.5)

            ax3.text(0.9,0.37,f'y = {slope3:.3f} x + {intercept3:.3f}',color = 'green',size = 20, ha = 'center',va = 'top',transform=fig.transFigure)
            ax3.text(0.9,0.34,f'P_value = {p_val3:.3f} \n \n R^2: {r_sq3:.3f} \n \n Adj_R^2: {adj_r3:.3f} \n \n FQ1_Trend_E = {est_3:.3f} \n\n FQ1_Rev_E = {rev_est:.3f}\n \n FQ1_Forecast = {forc3:.3f} \n \n FQ1_Surp = {surp3:.3f}',color = 'green',size = 20,ha = 'center',va = 'top',transform=fig.transFigure)
            
            
            if re.search(r'^.*_US.*$',cols_dict[key][0]):
                a = 'Google Trends US Plots'
                b = 'Google Trends US'
                print('\n')
                print(a)
                print('\n')
            elif re.search(r'^.*_Global.*$',cols_dict[key][0]):
                a = 'Google Trends Global Plots'
                b = 'Google Trends Global'
                print('\n')
                print(a)
                print('\n')
                
            else:
                a = f'{cols_dict[key][0].split(r"_")[0]} Plots'
                b = cols_dict[key][0].split(r"_")[0]
                print('\n')
                print(a)
                print('\n')
            print('\n')
            print('Scatter Plots:')
            print('\n')
            plt.show()
            
            #line plots
            if line == True: 
                cols_pseud = cols_dict[key].copy()
                cols_pseud.extend(['actual_flag','quarter_end_dates'])
                df_line = df_plts[cols_pseud]
                print('\n')
                print('Trend Plots:')
                print('\n')
                line_plots(df=df_line,b=b,cols_dict=cols_dict,key = key,ticker = ticker,hit_rates=hit_rates,accel_dict = accel_dict,wk_dict = wk_dict,mo_cols=[],df_weekly = df_w, df_monthly = df_m)
            #scatter matrix
            if matrix == True:
                cols_pseud = cols_dict[key].copy()
                cols_pseud.append('actual_flag')
                df_mat = df_plts[cols_pseud]
                print('\n')
                print('Scatter Matrix:')
                print('\n')
                scatter_matrix(df_mat)
             
      
            #ts 
            if decompose == True:
                try:
                    df_t = df_ts[cols_dict[key]]
                    df_t = df_t.dropna()
                    #get decomp ser 
                    ts_plots(df_t,cols_dict,key, b,ticker)
                except Exception as e:
                    print('Error During TS Decomposition of:', cols_dict[key][:2],e)
                    pass 
        
            if resid == True:
                resid_plts(df_t,cols_dict,key, b,ticker)
    
        except Exception as e:
            print(e)
            continue 
            
            


     
    


# # Earnings 

# In[329]:


def earn_out(file,file_out, header, to_display = None,wk_display = None, insta = True,line = True, full = False, engine = 'pyxlsb',flip = False,resid = False,matrix = False, combine = True,days = [],drop_dates = []): 
    
    pd.set_option("display.max_rows",None, "display.max_columns", None)
    if len(days) == 0: 
        days = ['Monday B4 Close','Tuesday B4 Close','Wednesday B4 Close','Thursday B4 Close']
    #read excel sheet 
    earn = pd.read_excel(file, sheet_name=days,header = header,engine= engine)
    #build dfs
    df_lst = []
    writer = pd.ExcelWriter(f'{file_out}.xlsx',date_format='YYYY-MM-DD', engine='xlsxwriter')
    writer_full = pd.ExcelWriter(f'{file_out}_Full.xlsx',date_format='YYYY-MM-DD', engine='xlsxwriter')


    to_display = to_display 
    drop_dates = drop_dates 
    for day in earn:

        df,quart,week = combine_screen(earn[day].Ticker.tolist(),to_display= to_display,wk_display = wk_display,insta = insta,earn= True)
        df_dis = df.drop('quarter',axis = 1)
        df_dis= df_dis.T
        df_dis.index.names = ['Columns','FQ']
        df_dis = qgrid.show_grid(df_dis,precision=2,grid_options = {'forceFitColumns': False, 'maxVisibleRows':60,
       'autoHeight': True, },show_toolbar = True)
        df_lst.append(df) 
        print('\n')
        compa = f'<h1>{day}:</h1>'
        display(HTML(compa))
        print('\n')
        compb = f'<h1>{day.split(" ")[0]} Tickers Full Data Frame:</h1>'
        display(HTML(compb))
        print('\n')
        display(df_dis)
        #plots
        print('\n')
        comp = f'<h1>Plots:</h1>'
        display(HTML(comp))
        print('\n')
        plot_main(quart.index,df = quart, line = line, df_weekly= week, drop_dates= drop_dates,resid = resid,matrix = matrix)
        #write df to excel
        df.to_excel(writer,sheet_name = day,encoding ='utf8')

    
    writer.save()
    if full == True:
        df_main = pd.concat(df_lst,axis =1)
        for t in df_main.index: 
            df_t = df_main.loc[t].to_frame().T.stack()
            df_t.to_excel(writer_fll, t.replace(':',''))
    writer_full.save()
            
        
        
        
        
    
    
    
    
    


# # Old versions 

# In[30]:


# def combine_screen(ticker_list, combine = False, display_quarters = None, flip = False): 
#   df_lst = []
#   missing = []
#   for ticker in ticker_list: 
#     try:
#       quart = screen_quarterly([ticker],flip = False)
#       month = screen_monthly([ticker],flip = False)
#       #combine 
#       if combine == True: 
#         #quarterly cols
#         cols = list(quart.columns.levels[:-1][0])
#         cols = [re.sub(r'quarterly_change',r'QM_change_Studio',x) for x in cols]
#         cols = [re.sub(r'weekly_change',r'weekly_change_Sentieo',x) for x in cols]
#         cols = [re.sub(r'daily_change',r'daily_change_Sentieo',x) for x in cols]
#         quart= quart.rename(columns={key:value for key, value in zip(list(quart.columns.levels[:-1][0]), cols)})

#         #monthly cols 
#         cols2 = list(month.columns.levels[:-1][0])
#         cols2 = [re.sub(r'monthly_change',r'QM_change_Studio',x) for x in cols2]
#         cols2 = [re.sub(r'monthly_aligned_change',r'change_Sentieo',x) for x in cols2]
#         month = month.rename(columns={key:value for key, value in zip(list(month.columns.levels[:-1][0]), cols2)})

#       #join 
#       df_joi = pd.concat([quart,month],axis =0, ignore_index= True)
#       df_joi = df_joi.fillna(method = 'bfill').drop(1,axis =0)
#       df_joi.index = [ticker]
#       df_lst.append(df_joi)
#     except Exception as e:
#       print(ticker,e)
#       missing.append(ticker)
#       continue

#   #merge all the dataframes together 
#   df_final = pd.concat(df_lst)
#   #first level cols 
#   cols1 = list(df_final.columns.levels[:-1][0])
#   cols1_main =  ['quarter_end_dates','Revenue_yoy_change']
#   cols1_other = [x for x in cols1 if x not in cols1_main]
#   cols1_main.extend(sorted(cols1_other))
#   #second level cols 
#   cols0 = df_final.columns.levels[-1].tolist()
#   cols0_stats= ['R^2_including_FQ0','PValue_including_FQ0','slope_including_FQ0','intercept_including_FQ0','R^2_excluding_FQ0','PValue_excluding_FQ0','slope_excluding_FQ0','intercept_excluding_FQ0',
#            'Adj_R^2_including_FQ0','Adj_R^2_excluding_FQ0', 'FQ1_Forecast_including_FQ0',  'FQ1_Forecast_excluding_FQ0','FQ2_Forecast_including_FQ0','FQ2_Forecast_excluding_FQ0',
#            'FQ1_Surp_including_FQ0','FQ1_Surp_excluding_FQ0','FQ2_Surp_including_FQ0',  'FQ2_Surp_excluding_FQ0' ]
#   cols0_qt = [x for x in cols0 if x not in cols0_stats]
#   cols0_act = [x for x in cols0_qt if re.search(r'^FQ.\d.*$',x)]
#   cols0_est = [x for x in cols0_qt if x not in cols0_act]
#   nums =[ ''.join(re.findall(r'FQ.\d.',x)) for x in cols0_act]
#   cols0_nums = [(int(''.join(re.findall(r'\d',num))),x) for num,x in zip(nums,cols0_act)]
  
 
#   cols0_nums.sort(key = lambda x: x[0], reverse = True)
#   cols0_est.sort(key = lambda x: x[2])
#   cols0_act = [y for x,y in cols0_nums]
#   cols0_main = cols0_act 
#   cols0_main.extend(cols0_est)
#   cols0_main.extend(cols0_stats)
#   df_final= df_final.reindex(cols1_main, axis = 1, level =0)
#   df_final = df_final.reindex(cols0_main,axis = 1, level = 1)
#   #limiting number of quarters 
#   if display_quarters != None:
#     n = display_quarters
#     want = [(x,y) for x,y in cols0_nums if x<=n]
#     want = [y for (x,y) in want]
#     want.extend(cols0_est)
#     want.extend(cols0_stats)
#     df_final = df_final.loc[:,(slice(None),want)]
    

#   display(missing)  
#   if flip == False: 
#     return df_final 
#   return df_final.stack().T





  


# In[31]:


# # fig = make_subplots(
# #     rows=1, cols=1,
# #     subplot_titles=( "2y"),shared_xaxes=True)
# fig = go.Figure()
# dff = dff.reset_index()
# dff = dff.set_index('quarter_end_dates')
# dff = dff.sort_index()
# fig.add_trace(go.Scatter(x = dff.index, y = dff[cols[4]], name = 'Revenue_yoy_change',mode='lines+markers'
#                     , hovertext=dff.FQ,yaxis='y',))

# fig.add_trace(go.Scatter(x = dff.index, y = dff[cols[1]],hovertext=dff.FQ, name = 'Alexa',yaxis='y2',
#                         mode = 'lines+markers',line = dict(color = 'black')))

# fig.add_trace(go.Scatter(x = dff.index, y = dff[cols[2]],hovertext=dff.FQ, name = 'Alexa2',yaxis='y2',
#                         mode = 'lines+markers', line=dict(color='red')))

# #fig.add_trace(go.Scatter(x = dff.quarter_end_dates, y = dff[cols[2]]), row = 1,col = 2)

# # fig.update_layout(
# #     xaxis = dict(rangeslider_visible=True,type ='date',
# #     rangeselector=dict(
# #         buttons=list([
# #             dict(count=1, label="1m", step="month", stepmode="backward"),
# #             dict(count=3, label="3m", step="month", stepmode="backward"),
# #             dict(count=6, label="6m", step="month", stepmode="backward"),
# #             dict(count=1, label="YTD", step="year", stepmode="todate"),
# #             dict(count=1, label="1y", step="year", stepmode="backward"),
# #             dict(step="all")])),
# #              ),
    
   
# #     yaxis=dict(
# #         title="Revenue_yoy",
# #         titlefont=dict(
# #             color="blue"
# #         ),
# #         tickfont=dict(
# #             color="blue"
# #         ),range= [-4.213658178052506, 34.637801831806826],
# #   dtick= 3.0,

        
       
        
  
        
# #     ),
# #     yaxis2=dict(
# #         title="Alexa Page",
# #         titlefont=dict(
# #             color="black"
# #         ),
# #         tickfont=dict(
# #             color="black"
# #         ),
        
# #         side="right",
# #      range= [-28.09105452035004, 230.91867887871217],
# #   dtick= 20.0, anchor= 'x',
# #   overlaying= 'y',

# #   position= 0.925

        
   

        
      
# #     )
# # )
# # Update layout properties
# fig.update_layout(xaxis=dict(domain=[0, right_y_position],
#                             rangeslider_visible=True,type ='date',
#     rangeselector=dict(#yanchor ='top',
#         buttons=list([
#             dict(count=1, label="1m", step="month", stepmode="backward"),
#             dict(count=3, label="3m", step="month", stepmode="backward"),
#             dict(count=6, label="6m", step="month", stepmode="backward"),
#             dict(count=1, label="YTD", step="year", stepmode="todate"),
#             dict(count=1, label="1y", step="year", stepmode="backward"),
#             dict(step="all")]))))
# fig.update_layout(yaxis_dict)

# fig.update_layout(
#     title_text=f"\n CRM:  Alexa Trends (Long Term Directions) Over Time",
#     width=1050,height = 600,legend=dict(
#     #yanchor="top",
#     y=1.21,
#     xanchor="right",
#     x=1)
# )
# fig.add_annotation(text=f'Hit Rates: <br> ',showarrow=False, visible = True,xref = 'paper',yref = 'paper',
#                        bordercolor='blue',
#                 borderwidth=3,y = -0.5,x = 0.45,align = 'left', width = 200,height = 50,font = dict(size = 19),
#                   xshift = 1)
    
# fig.show()


# In[32]:


# def plot_main(ticker_list,drop_dates= []): 
#   dic= scaled_combo(ticker_list,drop_dates)
#   missing = []
#   store = []
#   df_dic = defaultdict(list)

#   for ticker in dic.keys(): 
#     #get the Df for the ticker 
#     try:
#       df = dic[ticker]
#       df.set_index('quarter_end_dates',inplace = True)
#       df.replace(np.inf, np.nan,inplace = True)
#       print('\n')
#       print(f'Current Company: {ticker}')
#       print('\n')
    
#     except: 
#       missing.append(ticker)
#       continue

#     #fill any NANs with the mean 
#     #df.fillna(df.median(),inplace = True)
  
#     #print company iNFO
    
#     df_dic['Ticker'].append(ticker)
#     cols_used = []
    
#     #go through each column, find the fit and plot 
#     for column in df.columns.tolist():
#       if column == 'Revenue_yoy_change' or column == 'ticker' or column == 'actual_flag' or column == 'quarter_end_dates': 
#         continue
#       else: 

#         try:
#           #find series of interest 
#           curr_df = df.loc[(df['actual_flag'] == 'A') & (df[column] != np.inf) & (df[column]!= np.nan), [column,'Revenue_yoy_change']]
#           #filtering by count
#           curr_df.dropna(inplace = True)



#           cols_used.append(column)
#           curr_df.dropna(inplace = True)
#           y = np.array(curr_df['Revenue_yoy_change'].values,dtype = 'float64')
#           y2 = np.array(curr_df['Revenue_yoy_change'].values[:-1],dtype = 'float64')
          
#           #including current
#           x = np.array(curr_df[column].values,dtype = 'float64')
#           #excluding 
#           x2 = np.array(curr_df[column].values[:-1],dtype ='float64')
#           #find the fit and r_sq
#           #put the try except here maybe 
#           #first plot 
#           try:
#             slope, intercept = np.polyfit(x,y,deg = 1)
#             r_sq, p_value = stats.pearsonr(x,y)
          
#           #second plot 
#             slope2,intercept2 = np.polyfit(x2,y2,deg = 1)
#             r_sq2, p_value2 = stats.pearsonr(x2,y2)

#             df_dic[f'{column}_slope_including_FQ0'].append(slope)
#             df_dic[f'{column}_R^2_including_FQ0'].append(r_sq**2)
#             df_dic[f'{column}_PValue_including_FQ0'].append(p_value)

#             df_dic[f'{column}_slope_excluding_FQ0'].append(slope2)
#             df_dic[f'{column}_R^2_excluding_FQ0'].append(r_sq2**2)
#             df_dic[f'{column}_PValue_excluding_FQ0'].append(p_value2)

          
#             if column == 'Google Trends_global_yoy_weekly_change':
#               store.append((ticker,r_sq**2,p_value))
#           except: 
#             missing.append(ticker)
#             break
#         except ValueError:
#           print(column, ValueError) 
#           continue

#         #plotting 
#         #first plot
#         fig, axs = plt.subplots(1,2,figsize= (20,8))
#         #scatter plot 
#         df.plot(x = column, y = 'Revenue_yoy_change',kind= 'scatter',ax = axs[0],s = 80,color= 'black',marker = '+')
        
#         df.index = pd.to_datetime(df.index).strftime('%m/%Y')
        
#         for k,v in df.loc[df['actual_flag'] =='A',[column,'Revenue_yoy_change']].iterrows():
          
#           axs[0].annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
#                 family='sans-serif', fontsize=10, ha = 'left', va = 'top', color='black') 
        
#         for k,v in df.loc[df['actual_flag'] =='E',[column,'Revenue_yoy_change']].iterrows(): 
#           axs[0].annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
#                 family='sans-serif', fontsize=13, ha = 'left', va = 'top', color='blue')
#         #plotting trend lines
#         axs[0].plot(x,slope*x + intercept,linewidth =5,color = 'black')

#         axs[0].set_title(f'\n {ticker} \n \n Revenue vs. {column.split(r"_")[0]} \n {" ".join(column.split(r"_")[1:4])} \n Including FQ0 \n',fontsize = 19.5)
#         axs[0].text(-0.06,1,s =f'Slope: {slope:.3f} \n Intercept: {intercept:.3f}',color = 'black',size = 20, ha='center', va='top',transform=fig.transFigure )
#         axs[0].text(-0.06,0.9,s = f'Pearsons R: {r_sq:.3f} \n R^2: {r_sq**2:.3f} \n p_value = {p_value:.3f}',color = 'black',size = 20, ha='center', va='top',transform=fig.transFigure)


#         #second plot 

#         #scatter
#         df.plot(x = column, y = 'Revenue_yoy_change',kind= 'scatter',ax = axs[1],s =80,color= 'red',marker = 'o')

#         for k,v in df.loc[df['actual_flag'] =='A',[column,'Revenue_yoy_change']].iterrows():
#           axs[1].annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
#                 family='sans-serif', fontsize=10, ha = 'left', va = 'top', color='red')
#         for k,v in df.loc[df['actual_flag'] =='E',[column,'Revenue_yoy_change']].iterrows():
#           axs[1].annotate(k,v,xytext=(-10,15),xycoords ='data', textcoords='offset points',
#                 family='sans-serif', fontsize=13, ha = 'left', va = 'top', color='blue')
#         #plotting trend lines

#         axs[1].plot(x2,slope2*x2 + intercept2,linewidth = 5)

#         axs[1].set_title(f'\n {ticker} \n \n Revenue vs. {column.split(r"_")[0]} \n {" ".join(column.split(r"_")[1:4])} \n Excluding FQ0 \n',fontsize = 19.5)
#         axs[1].text(1.06,1,f'Slope: {slope2:.3f} \n Intercept {intercept2:.3f}',color = 'red',size = 20, ha = 'center',va = 'top',transform=fig.transFigure)
#         axs[1].text(1.06,0.9,f'Pearsons R: {r_sq2:.3f} \n R^2: {r_sq2**2:.3f} \n p_value: {p_value2:.3f}',color = 'red',size = 20,ha = 'center',va = 'top',transform=fig.transFigure)

        
         
#         plt.show()
#     # scatter_df = df.loc[:,cols_used]
#     # cols_used =[" ".join(x.split(r"_")[0:5]) for x in cols_used]
#     # scatter_df.columns = cols_used 
#     # sns.pairplot(scatter_df,vars =cols_used[:5],aspect=4,height = 3)
#     # print('\n')

#     #print('Table of R^2 including all data points:')
#    # print('\n')
    
  


    
    
#   print('\n')
#   store.sort(key = lambda x: (x[1],-x[2]),reverse = True)
#   print('Number of Companies Missing:')
#   display(len(missing))
#   print('List of Missing Value Companies:')
#   display(missing)
#   print('Companies Sorted by highest Coorelation with Trend and lowest P-value (ticker, R^2, P_value):')
#   display(store)

#   lst_dfs = [pd.DataFrame({key: value}) for key,value in df_dic.items()]
#   df_stats= pd.concat(lst_dfs, axis = 1)
#   #display(df_stats)
#   return df_stats



  






       

        






  







# In[33]:


# def monthly_df(ticker, to_display = None,insta = True):
#     df_dic = get_dfs(ticker, insta = insta)
#     #get revenue data frame and calc pct_change 
#     rev_df = df_dic['Revenue'].copy()
#     #pct_changes 
#     rev_df['Revenue_yoy_2Ychange'] = rev_df['Total Revenue'].pct_change(8) * 100
#     rev_df['Total Revenue'] = rev_df['Total Revenue'].pct_change(4)
#     rev_df['Total Revenue'] = rev_df['Total Revenue'] * 100
#     rev_df = rev_df.rename(columns ={'Total Revenue': 'Revenue_yoy_change'})
#     rev_df = rev_df.reset_index()
#     df_lst = []
#     #go through each trend 
#     for key in df_dic: 
#         if key== 'Revenue': 
#             continue 
#         else: 
#             try:
#                 trend = df_dic[key]
#                 trend_name = trend.select_dtypes(include = ['float64','int64']).columns[0]
#                 #monthly calcs
#                 month = monthly_yoy(trend[trend_name])


#                 #daily 
#                 if (trend.reset_index()['date'] - trend.reset_index()['date'].shift(1)).mode()[0] == timedelta(days = 1):
#                     daily = daily_yoy(trend[trend_name])
#                     daily = daily.resample('1M').mean()
#                     daily.name = f'{trend_name}_daily_monthly_aligned_yoy_change'
#                     df = pd.DataFrame(daily)
#                 else:
#                     weekly = weekly_yoy(trend[trend_name])
#                     weekly = weekly.resample('1M').mean()
#                     weekly.name = f'{trend_name}_weekly_monthly_aligned_yoy_change'
#                     df = pd.DataFrame(weekly)
#             except Exception as e: 
#                 print(f'Error in the montly series calculation for series: {key}',e)
#                 continue
                
            
#             #merge the trends
#             df.index.set_names('date',inplace = True)
#             df[f'{trend_name}_monthly_yoy_change'] = month 
#             df['quarter'] = pd.PeriodIndex(df.index,freq = trend.quarter.dt.freq)
#             df= df.reset_index()
#             df = pd.merge(rev_df, df, on='quarter',how = 'left')
#             df = df.set_index(['quarter','quarter_end_dates','date','ticker','actual_flag','Revenue_yoy_change','Revenue_yoy_2Ychange'])
#             df_lst.append(df)
           
#     #set up index 
#     lens = [len(df) for df in df_lst]
#     df_final = pd.concat(df_lst,axis = 1)
#     #reset indecies 
#     df_final = df_final.reset_index()
#     #build FQ 
#     act = df_final.loc[df_final.actual_flag == 'A']
#     est = df_final.loc[df_final.actual_flag == 'E']
#     q_act = list(set(act.quarter)) 
#     q_est = list(set(est.quarter))
#     q_act.sort()
#     q_est.sort()
    
#     map_act = {q_act:f'FQ{x}' for q_act,x in zip(q_act,range(-len(q_act)+1,1))}
#     map_est = {q_est:f'FQ{x}' for q_est,x in zip(q_est,range(1, len(q_est)+1))}
#     map_act.update(map_est)
    
#     df_final['FQ'] =df_final.quarter.replace(map_act)
#     #set indecies 
#     df_final = df_final.set_index(['quarter','quarter_end_dates','date'])
     
#     #order cols 
    
#     cols = df_final.columns.tolist()
#     main_cols= [ 'FQ','ticker', 'Revenue_yoy_change','Revenue_yoy_2Ychange', 'actual_flag']
#     cols = [x for x in cols if x not in main_cols]
#     #order data stream cols 
#     main_cols.extend(sorted(cols))
#     df_final = df_final.reindex(main_cols,axis = 1)
#     df_final = df_final.dropna(thresh = 6)
#     #display 
#     if to_display != None:
#         df_final = df_final.reset_index()
#         df_final = quart_num(df_final,to_display = to_display,quart = False)
#         df_final = df_final.set_index(['quarter','quarter_end_dates','date'])
    
#     return df_final 
                
                
            
    


# In[34]:


# def quarterly_df(ticker,drop_dates =[],to_display = None,insta = True): 
#     df_dic = get_dfs(ticker,insta = insta)
#     #get revenue data frame and calc pct_change 
#     rev_df = df_dic['revenue_df'].copy()
#     #pct_changes 
#     rev_df['Revenue_yoy_2Ychange'] = rev_df['Total Revenue'].pct_change(8) * 100
#     rev_df['Total Revenue'] = rev_df['Total Revenue'].pct_change(4)
#     rev_df['Total Revenue'] = rev_df['Total Revenue'] * 100
    
#     rev_df = rev_df.rename(columns ={'Total Revenue': 'Revenue_yoy_change'})
    
#     #go through each trend 
#     for key in df_dic: 
#         if key== 'Revenue': 
#             continue 
#         else: 
            
#             try:
#                 trend = df_dic[key]
#                 trend_name = trend.select_dtypes(include = ['float64','int64']).columns[0]
#                 #quarterly 
#                 quart= quarterly_yoy(trend[[trend_name,'quarter_end_dates','quarter']])
#                 quart = quart.rename(columns = {trend_name:f'{trend_name}_quarterly_yoy_change'})

#                 if (trend.reset_index()['date'] - trend.reset_index()['date'].shift(1)).mode()[0] == timedelta(days = 1):
#                     #daily trends  
#                     trend[f'{trend_name}_daily_yoy_change'] = daily_yoy(trend[trend_name])
#                     trend[f'{trend_name}_daily_yoy_2Ychange'] = daily_2yoy(trend[trend_name])
#                     trend = trend.groupby(['quarter','quarter_end_dates'])[[f'{trend_name}_daily_yoy_change',f'{trend_name}_daily_yoy_2Ychange']].mean()
#                     quart[f'{trend_name}_daily_yoy_change'] = trend[f'{trend_name}_daily_yoy_change']
#                     quart[f'{trend_name}_daily_yoy_2Ychange'] = trend[f'{trend_name}_daily_yoy_2Ychange']

#                 #weekly trends 
#                 else: 
#                     trend[f'{trend_name}_weekly_yoy_change'] = weekly_yoy(trend[trend_name])
#                     trend[f'{trend_name}_weekly_yoy_2Ychange'] = weekly_2yoy(trend[trend_name])
#                     trend =trend.groupby(['quarter','quarter_end_dates'])[[f'{trend_name}_weekly_yoy_change',f'{trend_name}_weekly_yoy_2Ychange']].mean()
#                     quart[f'{trend_name}_weekly_yoy_change'] = trend[f'{trend_name}_weekly_yoy_change']
#                     quart[f'{trend_name}_weekly_yoy_2Ychange'] = trend[f'{trend_name}_weekly_yoy_2Ychange']

#                 #merge 
#                 rev_df = pd.merge(rev_df,quart,left_index =True, right_index = True,how = 'left')
#             except Exception as e: 
#                 print(f'Error during a series calc, series: {key}',e)
#                 continue
                
#     #date dropping 
#     if len(drop_dates) != 0:
        
#         #if in YQ format 
#         if len(re.findall(r'^\d.*Q.*$',drop_dates[0])) != 0: 
#             rev_df = rev_df.drop(rev_df.loc[rev_df.quarter.isin(pd.PeriodIndex(drop_dates,freq=rev_df.quarter.dt.freq))].index)
        
        
#         #if in m/Y format or Y/m
#         elif len(re.findall(r'^\d{2}/\d{4}$',drop_dates[0])) != 0 or len(re.findall(r'^\d{4}/\d{2}$',drop_dates[0])) !=0:
#             rev_df = rev_df.drop(rev_df.loc[(rev_df.index.strftime('%m/%Y').isin(drop_dates))|(rev_df.index.strftime('%Y/%m').isin(drop_dates)) ,:].index)
        
        
#         #quarter_end_date format
#         else:
#             drop_dates = [datetime.strptime(x,'%Y-%m-%d') for x in drop_dates]
#             rev_df = rev_df(drop_dates)
            
    
#     rev_df = rev_df.drop('quarter_end_dates',axis =1 )
#     #add FQ col 
#     act = rev_df.loc[rev_df.actual_flag == 'A',:].index
#     est = rev_df.loc[rev_df.actual_flag == 'E',:].index
#     fq_act = [f'FQ{i}_A' for i in range(-len(act)+1,1)]
#     fq_est = [f'FQ{i}_E' for i in range(1,len(est)+1)]
#     fq_act.extend(fq_est)
#     rev_df['FQ'] = fq_act
#     #order columns
#     cols = rev_df.columns.tolist()
#     main_cols= [ 'FQ','ticker', 'Revenue_yoy_change','Revenue_yoy_2Ychange', 'actual_flag']
#     cols = [x for x in cols if x not in main_cols]
#     #order data stream cols 
#     main_cols.extend(sorted(cols))
#     rev_df = rev_df.reindex(main_cols,axis = 1)
#     rev_df = rev_df.dropna(thresh = 6)
#     #display
#     if to_display != None:
#         rev_df = quart_num(rev_df,to_display = to_display)
#     return rev_df
            
                


# In[35]:


# def screen_quarterly(ticker_list,flip = False,drop_dates = []):
    
#     #Get dict of dfs 
#     df_dic = scaled_combo(ticker_list,drop_dates)
#     df_lst = []
#     col_lens = []
#     missing = []
    
#     for ticker in ticker_list:
#         try:
#             #build df 
#             df = df_dic[ticker]
#             #build index 
#             index = [f'{x[0]}_{f}' for x,f in zip(df.index.tolist(),df.actual_flag.tolist())]
#             #reset_index 
#             df = df.reset_index()
#             #drop cols 
#             df = df.drop(['quarter','actual_flag','ticker'],axis = 1)
#             #set index 
#             df.index = index 
#             #unstack and build 
#             df = df.unstack().to_frame().T
#             #build stats df 
#             df_stats = stats_df(df_dic[ticker])
#             df_main = pd.concat([df,df_stats],axis = 0,join = 'outer',ignore_index=True)
#             if len(df_stats)!= 0:
#                 df_main = df_main.fillna(method = 'bfill').drop(1,axis =0)
            
#             #set index 
#             df_main.index = [ticker]
#             #append
#             df_lst.append(df_main)
#         except Exception as e:
#             print(ticker,e)
#             missing.append(ticker)
#             continue
    
#     display(missing)
#     df_final = pd.concat(df_lst)
#     #order cols
#     #level 0
#     cols = list(df_final.columns.levels[:-1][0])
#     cols_main =  ['quarter_end_dates','Revenue_yoy_change']
#     cols_other = [x for x in cols if x not in cols_main]
#     cols_main.extend(sorted(cols_other))
    
#     #level 1
#     cols1 = df_final.columns.levels[-1].tolist()
#     stats_cols = ['R^2_including_FQ0','PValue_including_FQ0','slope_including_FQ0','intercept_including_FQ0','R^2_excluding_FQ0','PValue_excluding_FQ0','slope_excluding_FQ0','intercept_excluding_FQ0',
#            'Adj_R^2_including_FQ0','Adj_R^2_excluding_FQ0', 'FQ1_Forecast_including_FQ0',  'FQ1_Forecast_excluding_FQ0','FQ2_Forecast_including_FQ0','FQ2_Forecast_excluding_FQ0',
#            'FQ1_Surp_including_FQ0','FQ1_Surp_excluding_FQ0','FQ2_Surp_including_FQ0',  'FQ2_Surp_excluding_FQ0' ]
    
#     cols1_main = [x for x in cols1 if x not in stats_cols]
#     cols1_main.extend(stats_cols)
    
    
#     df_final = df_final.reindex(cols_main,axis =1,level = 0)
#     df_final = df_final.reindex(cols1_main,axis =1,level = 1)
#     if flip == False:
#         df_final = df_final.dropna(axis = 1,how ='all')
#         return df_final
#     else: 
#         return df_final.stack().T
            
#  ##TODO: displays with quarter_arg, and maybe            

