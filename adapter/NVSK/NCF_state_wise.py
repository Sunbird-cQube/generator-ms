from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def no_of_participants():
    df_snap = df_data[['State Code', 'Number of Participants']]
    df_snap.columns = ['state_id', 'number_of_participants']
    obj.upload_file(df_snap, 'statewiseparticipants-event.data.csv')

if df_data is not None:
    no_of_participants()
