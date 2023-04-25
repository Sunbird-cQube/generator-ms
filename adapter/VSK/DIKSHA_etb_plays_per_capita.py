from main import CollectData
from datetime import datetime

obj=CollectData()
program=obj.program
df_data=obj.get_file()
if df_data is not None:
    col_list = df_data.columns.tolist()
    final_col_list=[]
    date=None
    month=None
    for i in col_list:
        if 'Plays per capita' in i:
            slice=i.split(' (')
            date_slice=slice[1].strip(')')
            date_slice=date_slice.lstrip()
            date_slice = date_slice.replace('st', '').replace('nd', '').replace('rd', '').replace('th','')  # Remove ordinal indicators
            date_obj = datetime.strptime(date_slice, '%d %B %Y')
            date = datetime.strftime(date_obj, '%d/%m/%y')
            final_col_list.append(slice[0])
        else:
            final_col_list.append(i)
    df_data = df_data[col_list]
    df_data.columns = final_col_list
    df_data['date']=date

def plays_per_capita():
    df_snap = df_data[['date','State Code','Plays per capita']]
    df_snap.columns = ['date','state_id','plays_per_capita']
    obj.upload_file(df_snap, 'playspercapita-event.data.csv')

if df_data is not None:
    plays_per_capita()