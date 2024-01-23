from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def total_certificates_achieved():
    df_snap = df_data[['state_code','program_name','total_certifications_achieved']]
    df_snap.columns = ['state_id','program_name','total_certifications_achieved']
    obj.upload_file(df_snap, 'achievedcertification-event.data.csv')

def total_target_certificates():
    df_snap = df_data[['state_code','program_name','total_target_certifications']]
    df_snap.columns = ['state_id','program_name','total_target_certifications']
    obj.upload_file(df_snap, 'targetcertification-event.data.csv')    

if df_data is not None:
    total_certificates_achieved()
    total_target_certificates()
