from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def qr_coverage():
    df_snap = df_data[['textbook_id','grade','subject','medium','qr_coverage']]
    obj.upload_file(df_snap, 'qrcoverage-event.data.csv')

def qr_code_linked_to_content():
    df_snap = df_data[['textbook_id','grade','subject','medium','qr_codes_linked_to_content']]
    obj.upload_file(df_snap, 'contentqrcode-event.data.csv')

def total_qr_codes():
    df_snap = df_data[['textbook_id','grade','subject', 'medium','total_qr_codes']]
    obj.upload_file(df_snap, 'totalqrcodes-event.data.csv')

def textbook_dimension():
    df_snap = df_data[['textbook_id', 'textbook_name']].drop_duplicates()
    obj.upload_file(df_snap,'textbookdiksha-dimension.data.csv')

if df_data is not None:
    qr_coverage()
    qr_code_linked_to_content()
    total_qr_codes()
    textbook_dimension()


