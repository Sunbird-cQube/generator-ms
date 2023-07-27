from main import CollectData

obj = CollectData()
program = obj.program
df_data = obj.get_file()


def totalcertification_event_data():
    df_snap = df_data[['program_name','course_name','total_certification']]
    obj.upload_file(df_snap, 'totalcertification-event.data.csv')

def course_dimension():
    df_snap = df[['course_id', 'course_name']].drop_duplicates()
    obj.upload_file(df_snap, 'coursenishtha-dimension.data.csv')


if df_data is not None:
    totalcertification_event_data()
    course_dimension()
