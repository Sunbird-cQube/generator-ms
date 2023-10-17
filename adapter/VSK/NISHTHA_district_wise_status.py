from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def total_certificates_achieved():
    df_snap = df_data[['state_code','program_name','total_certifications_achieved']]
    df_snap.columns = ['state_id','program_name','total_certifications_achieved']
    obj.upload_file(df_snap, 'achievedcertification-event.data.csv')

def program_dimension_data():
    df_snap = df_data[['program_name']].drop_duplicates()
    df_snap['program_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['program_id', 'program_name']]
    df_snap.update(df_snap[['program_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'programnishtha-dimension.data.csv')

if df_data is not None:
    total_certificates_achieved()
    program_dimension_data()