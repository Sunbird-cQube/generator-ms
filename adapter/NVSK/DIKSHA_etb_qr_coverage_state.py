from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def qr_coverage():
    df_snap = df_data[['state_code', 'qr_coverage']]
    df_snap.columns = ['state_id', 'qr_coverage']
    obj.upload_file(df_snap, 'etbqrcoverage-event.data.csv')

def qr_covered():
    df_snap = df_data[['state_code','qr_covered']]
    df_snap.columns = ['state_id','qr_covered']
    obj.upload_file(df_snap, 'etbqrcovered-event.data.csv')

def total_qr_count():
    df_snap = df_data[['state_code', 'total_qr_count']]
    df_snap.columns = ['state_id', 'total_qr_count']
    obj.upload_file(df_snap, 'etbtotalqrcount-event.data.csv')

if df_data is not None:
    qr_coverage()
    qr_covered()
    total_qr_count()


