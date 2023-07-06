from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def energised_textbooks():
    df_snap = df_data[['state_code','energised_textbooks']]
    df_snap.columns = ['state_id','energized_textbooks']
    obj.upload_file(df_snap, 'energizedtextbooks-event.data.csv')

if df_data is not None:
    energised_textbooks()
