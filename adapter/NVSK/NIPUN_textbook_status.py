from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def perc_los_covered():
    df_snap = df_data[['textbook_name', '%_los_covered']]
    df_snap.columns = ['textbook_name', 'perc_los_covered']
    obj.upload_file(df_snap, 'loscoveredperc-event.data.csv')

def textbook_dimension():
    df_snap = df_data[['textbook_name']].drop_duplicates()
    df_snap['textbook_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['textbook_id', 'textbook_name']]
    df_snap.columns = ['textbook_id', 'textbook_name']
    df_snap.update(df_snap[['textbook_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'textbooknipun-dimension.data.csv')

if df_data is not None:
    perc_los_covered()
    textbook_dimension()