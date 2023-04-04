from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def energised_textbooks():
    df_snap = df_data[['State Code','Energised textbooks (State & NCERT adopted)']]
    df_snap.columns = ['state_id','energised_textbooks_state_and_ncert_adopted']
    obj.upload_file(df_snap, 'energisedtextbooks-event.data.csv')


if df_data is not None:
    energised_textbooks()
