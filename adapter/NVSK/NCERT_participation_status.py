from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def total_enrollments():
    df_snap = df_data[['State Code', 'Quiz Name','Total Enrollments']]
    df_snap.columns = ['state_id','quiz_name','total_enrolments']
    obj.upload_file(df_snap, 'totalenrolments-event.data.csv')

def quiz_dimension():
    df_snap = df_data[['Quiz Name']].drop_duplicates()
    df_snap['quiz_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['quiz_id', 'Quiz Name']]
    df_snap.columns = ['quiz_id', 'quiz_name']
    df_snap.update(df_snap[['quiz_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'quizncert-dimension.data.csv')


if df_data is not None:
    total_enrollments()
    quiz_dimension()


