from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def total_plays():
    df_snap = df_data[['state_code','grade','subject','medium','total_no_of_plays']]
    df_snap.columns = ['state_id','grade','subject','medium','total_no_of_plays']
    obj.upload_file(df_snap, 'totalplays-event.data.csv')

def avg_play_time():
    df_snap = df_data[['state_code','grade','subject','medium','average_play_time']]
    df_snap.columns = ['state_id','grade','subject','medium','average_play_time']
    obj.upload_file(df_snap, 'avgplaytime-event.data.csv')

if df_data is not None:
    total_plays()
    avg_play_time()
