from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def total_live_tb():
    df_snap = df_data[['State Code','Total Live TB']]
    df_snap.columns = ['state_id','total_live_tb']
    obj.upload_file(df_snap, 'totallivetb-event.data.csv')

def total_energised_live_etb():
    df_snap = df_data[['State Code','Total Energised (Live ETB)']]
    df_snap.columns = ['state_id','total_energised_live_etb']
    obj.upload_file(df_snap, 'totalenergisedliveetb-event.data.csv')

def state_energised_etb():
    df_snap = df_data[['State Code','State energised (ETB)']]
    df_snap.columns = ['state_id','state_energised_etb']
    obj.upload_file(df_snap, 'stateenergisedetb-event.data.csv')

def total_curriculum_textbooks():
    df_snap = df_data[['State Code','Total Physical textbooks excluding adopted (Curriculum+Supplementary)']]
    df_snap.columns = ['state_id','total_curriculum_textbook']
    obj.upload_file(df_snap, 'totalcurriculumtextbook-event.data.csv')

def perc_etb_coverage():
    df_snap = df_data[['State Code','ETB Coverage %']]
    df_snap.columns = ['state_id','perc_etb_coverage']
    obj.upload_file(df_snap, 'etbcoverageperc-event.data.csv')

def etb_coverage():
    df_snap = df_data[['State Code','ETB Coverage']]
    df_snap.columns = ['state_id','etb_coverage']
    obj.upload_file(df_snap, 'etbcoverage-event.data.csv')

if df_data is not None:
    total_live_tb()
    total_energised_live_etb()
    state_energised_etb()
    total_curriculum_textbooks()
    perc_etb_coverage()
    etb_coverage()