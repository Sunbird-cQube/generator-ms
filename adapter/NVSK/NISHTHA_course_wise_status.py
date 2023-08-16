from main import CollectData

obj = CollectData()
program = obj.program
df_data = obj.get_file()


def totalcertification_event_data():
    df_snap = df_data[['program_name','course_name','total_certification']]
    obj.upload_file(df_snap, 'totalcertification-event.data.csv')

def course_dimension():
    df_snap = df_data[['course_name']].drop_duplicates()
    df_snap['course_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['course_id','course_name']]
    df_snap.update(df_snap[['course_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'coursenishtha-dimension.data.csv')


if df_data is not None:
    totalcertification_event_data()
    course_dimension()
