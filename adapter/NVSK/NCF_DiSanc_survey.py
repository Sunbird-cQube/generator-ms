from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def no_of_participants_language():
    df_snap = df_data[['language', 'number_of_participants']]
    df_snap.columns = ['language_name', 'number_of_participants']
    obj.upload_file(df_snap, 'languagewisecount-event.data.csv')

def language_dimension():
    df_snap = df_data[['language']].drop_duplicates()
    df_snap['language_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['language_id', 'language']]
    df_snap.columns = ['language_id', 'language_name']
    df_snap.update(df_snap[['language_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'languagencf-dimension.data.csv')

if df_data is not None:
    no_of_participants_language()
    language_dimension()
