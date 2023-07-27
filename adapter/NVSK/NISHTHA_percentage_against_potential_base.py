from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def actual_enrolment():
    df_snap = df_data[['state_code','program','total_enrolments']]
    df_snap.columns = ['state_id','program_name','actual_enrolment']
    obj.upload_file(df_snap, 'actualenrolment-event.data.csv')

def actual_certification():
    df_snap = df_data[['state_code', 'program','total_certificates_issued']]
    df_snap.columns = ['state_id','program_name','actual_certification']
    obj.upload_file(df_snap, 'actualcertification-event.data.csv')


def target_achieved_enrolment():
    df_snap = df_data[['state_code','program','%_target_achieved_enrolment']]
    df_snap.columns = ['state_id','program_name','perc_target_achieved_enrolment']
    obj.upload_file(df_snap, 'targetachievedenrolment-event.data.csv')

def target_achieved_certificates():
    df_snap = df_data[['state_code', 'program','%_target_achieved_certificates']]
    df_snap.columns = ['state_id','program_name', 'perc_target_achieved_certificates']
    obj.upload_file(df_snap, 'targetachievedcertificates-event.data.csv')

def expected_enrolment():
    df_snap = df_data[['state_code','program','total_expected_enrolment']]
    df_snap.columns = ['state_id','program_name','expected_enrolment']
    obj.upload_file(df_snap, 'expectedenrolment-event.data.csv')

def expected_certification():
    df_snap = df_data[['state_code', 'program','total_expected_certification']]
    df_snap.columns = ['state_id','program_name','total_expected_certificates']
    obj.upload_file(df_snap, 'expectedcertificates-event.data.csv')

if df_data is not None:
    actual_certification()
    actual_enrolment()
    target_achieved_enrolment()
    target_achieved_certificates()
    expected_enrolment()
    expected_certification()


