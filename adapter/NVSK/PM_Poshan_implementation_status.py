from main import CollectData

obj=CollectData()
program=obj.program
df_data = obj.get_file()

def onboarded_event_data():
    df_snap = df_data[['state_code', 'onboarded_on_pmposhan']]
    df_snap.columns = ['state_id', 'started']
    obj.upload_file(df_snap, 'programstarted-event.data.csv')

if df_data is not None:
    onboarded_event_data()
