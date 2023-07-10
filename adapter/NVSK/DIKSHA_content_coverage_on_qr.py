from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def qr_coverage():
    df_snap = df_data[['state_code', 'qr_coverage']]
    df_snap.columns = ['state_id', 'content_coverage_on_qr']
    obj.upload_file(df_snap, 'contentcoverage-event.data.csv')


if df_data is not None:
    qr_coverage()



