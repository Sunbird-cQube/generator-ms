from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()
col_list = df_data.columns.tolist()
final_col_list=[]
date=None
month=None
for i in col_list:
    if 'Plays per capita' in i:
        slice=i.split(' (')
        date=slice[1].strip(')')
        date=date.lstrip()
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