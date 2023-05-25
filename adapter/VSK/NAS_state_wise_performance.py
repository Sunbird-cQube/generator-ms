from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def school_event_data():
    df_snap = df_data[['State Code','District Code','Number of Schools']].drop_duplicates()
    df_snap.columns = ['state_id','district_id','no_of_schools']
    obj.upload_file(df_snap, 'schools-event.data.csv')

def teacher_event_data():
    df_snap = df_data[['State Code','District Code','Number of Teachers']].drop_duplicates()
    df_snap.columns = ['state_id','district_id','no_of_teachers']
    obj.upload_file(df_snap, 'teachers-event.data.csv')

def student_event_data():
    df_snap = df_data[['State Code', 'District Code', 'Students Surveyed']].drop_duplicates()
    df_snap.columns = ['state_id', 'district_id', 'students_surveyed']
    obj.upload_file(df_snap, 'studentssurveyed-event.data.csv')

def performance_event_data():
    df_snap = df_data[['State Code','District Code','Grade','Subject','Learning Outcome Code','Performance']].drop_duplicates()
    df_snap.columns = ['state_id','district_id','grade','subject','lo_code','performance']
    obj.upload_file(df_snap, 'performance-event.data.csv')

def learning_outcome_dimension_data():
    df_snap = df_data[['Grade','Subject','Learning Outcome Code','Learning Outcome']].drop_duplicates()
    df_snap.columns = ['grade','subject','lo_code','lo_name']
    obj.upload_file(df_snap, 'lo-dimension.data.csv')

if df_data is not None:
    school_event_data()
    teacher_event_data()
    student_event_data()
    performance_event_data()
    learning_outcome_dimension_data()

