from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def school_event_data():
    df_snap = df_data[['State Code','District Code','Grade','Subject','Learning Outcome Code','Number of Schools']].drop_duplicates()
    df_snap.columns = ['state_id','district_id','grade','subject','lo_code','no_of_schools']
    obj.upload_file(df_snap, 'schools-event.data.csv')

def teacher_event_data():
    df_snap = df_data[['state_code','district_code','grade','subject','learning_outcome_code','number_of_teachers']].drop_duplicates()
    df_snap.columns = ['state_id','district_id','grade','subject','lo_code','no_of_teachers']
    obj.upload_file(df_snap, 'teachers-event.data.csv')

def student_event_data():
    df_snap = df_data[['state_code','district_code','grade','subject','learning_outcome_code','students_surveyed']].drop_duplicates()
    df_snap.columns = ['state_id', 'district_id','grade','subject','lo_code','students_surveyed']
    obj.upload_file(df_snap, 'studentssurveyed-event.data.csv')

def performance_event_data():
    df_snap = df_data[['state_code','district_code','grade','subject','learning_outcome_code','performance']].drop_duplicates()
    df_snap.columns = ['state_id','district_id','grade','subject','lo_code','performance']
    obj.upload_file(df_snap, 'performance-event.data.csv')

def learning_outcome_dimension_data():
    df_snap = df_data[['learning_outcome_code', 'grade', 'subject', 'learning_outcome']].drop_duplicates()
    df_snap.columns = ['lo_code', 'grade', 'subject', 'lo_name']
    obj.upload_file(df_snap, 'lo-dimension.data.csv')

if df_data is not None:
    school_event_data()
    teacher_event_data()
    student_event_data()
    performance_event_data()
    learning_outcome_dimension_data()

