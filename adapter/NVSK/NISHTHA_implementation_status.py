from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def started_event_data():
    df_snap = df_data[['state_code','program_name','program_status','started']]
    df_snap.columns = ['state_id','program_name','program_status','started']
    obj.upload_file(df_snap, 'programstarted-event.data.csv')

def program_dimension_data():
    df_snap = df_data[['program_name']].drop_duplicates()
    df_snap['program_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['program_id', 'program_name']]
    df_snap.update(df_snap[['program_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'programnishtha-dimension.data.csv')

def program_status_dimension_data():
    df_snap = df_data[['program_status']].drop_duplicates()
    df_snap['program_status_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['program_status_id', 'program_status']]
    df_snap.update(df_snap[['program_status_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'programstatusnishtha-dimension.data.csv')

if df_data is not None:
    started_event_data()
    program_dimension_data()
    program_status_dimension_data()


