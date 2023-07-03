from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def learning_sessions():
    df_snap = df_data[['grade','subject','total_no_of_plays']]
    df_snap.columns = ['grade','subject','total_no_of_learning_sessions']
    obj.upload_file(df_snap, 'totallearningsessions-event.data.csv')


if df_data is not None:
    learning_sessions()