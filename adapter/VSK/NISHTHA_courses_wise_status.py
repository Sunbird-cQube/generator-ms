from main import CollectData

obj = CollectData()
program = obj.program
df_data = obj.get_file()

def totalenrolment_event_data():
    df_snap = df_data[['state_code', 'program','course_name', 'enrollments']]
    df_snap.columns = ['state_id','program_name','course_name','total_enrolment']
    obj.upload_file(df_snap, 'courseenrolment-event.data.csv')

def totalcompletion_event_data():
    df_snap = df_data[['state_code','program','course_name','completion']]
    df_snap.columns = [ 'state_id','program_name','course_name','total_completion']
    obj.upload_file(df_snap, 'coursecompletion-event.data.csv')

def totalcertification_event_data():
    df_snap = df_data[['state_code','program','course_name', 'certification']]
    df_snap.columns = ['state_id','program_name','course_name','total_certification']
    obj.upload_file(df_snap, 'coursecertification-event.data.csv')

def course_dimension():
    df_snap = df_data[['course_name']].drop_duplicates()
    df_snap['course_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['course_id', 'course_name']]
    df_snap.columns = ['course_id', 'course_name']
    df_snap.update(df_snap[['course_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'coursenishtha-dimension.data.csv')

if df_data is not None:
    totalenrolment_event_data()
    totalcompletion_event_data()
    totalcertification_event_data()
    course_dimension()
