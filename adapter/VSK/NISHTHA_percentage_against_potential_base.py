from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def total_participants():
    df_snap = df_data[['program_name','total_participants']]
    obj.upload_file(df_snap,'totalparticipants-event.data.csv')

if df_data is not None:
    total_participants()


