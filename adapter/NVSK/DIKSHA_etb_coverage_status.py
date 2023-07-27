from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()



def total_energised_tb():
    df_snap = df_data[['state_code','total_energised']]
    df_snap.columns = ['state_id','total_energized_textbooks']
    obj.upload_file(df_snap, 'totalenergizedtb-event.data.csv')

def total_curriculum_textbooks():
    df_snap = df_data[['state_code','total_physical_textbooks_excluding_adopted']]
    df_snap.columns = ['state_id','total_curriculum_textbooks']
    obj.upload_file(df_snap, 'totalcurriculumtb-event.data.csv')

def perc_etb_coverage():
    df_snap = df_data[['state_code','etb_coverage_%']]
    df_snap.columns = ['state_id','perc_energized_textbooks']
    obj.upload_file(df_snap, 'percenergizedtb-event.data.csv')


if df_data is not None:
    total_energised_tb()
    total_curriculum_textbooks()
    perc_etb_coverage()
