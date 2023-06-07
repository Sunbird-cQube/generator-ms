from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def plays_per_capita():
    df_snap = df_data[['state_code','plays_per_capita']]
    df_snap.columns = ['state_id','plays_per_capita']
    obj.upload_file(df_snap, 'playspercapita-event.data.csv')

if df_data is not None:
    plays_per_capita()