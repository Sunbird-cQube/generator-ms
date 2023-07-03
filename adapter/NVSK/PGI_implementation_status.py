from main import CollectData

obj=CollectData()
program=obj.program
df_data = obj.get_file()

def started_event_data():
    df_snap = df_data[['state_code', 'started']]
    df_snap.columns = ['state_id', 'started']
    obj.upload_file(df_snap, 'programstarted-event.data.csv')

if df_data is not None:
    started_event_data()
