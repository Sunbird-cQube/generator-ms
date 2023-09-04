from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def medium_instruction_event_data():
    df_snap = df_data[['district_code','program_name','language','status']]
    df_snap.columns = ['district_id','program_name','language','status']
    obj.upload_file(df_snap, 'totalmedium-event.data.csv')

def language_dimension():
    df_snap = df_data[['language']].drop_duplicates()
    df_snap['language_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['language_id', 'language']]
    df_snap.update(df_snap[['language_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'languagenishtha-dimension.data.csv')

if df_data is not None:
    medium_instruction_event_data()
    language_dimension()



