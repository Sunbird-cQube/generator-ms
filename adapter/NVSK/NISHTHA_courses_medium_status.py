from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def totalmedium_event_data():
    df_snap = df_data[['state_code','program_name','total_medium']]
    df_snap.columns = ['state_id','program_name','total_medium']
    obj.upload_file(df_snap, 'totalmedium-event.data.csv')

def totalcourses_event_data():
    df_snap = df_data[['state_code','program_name','total_courses']]
    df_snap.columns = ['state_id','program_name','total_courses_launched']
    obj.upload_file(df_snap, 'totalcourseslaunched-event.data.csv')

if df_data is not None:
    totalmedium_event_data()
    totalcourses_event_data()


