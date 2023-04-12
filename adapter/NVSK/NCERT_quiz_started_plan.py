from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def total_enrolments():
    df_snap = df_data[['Collection Name', 'Medium','Total Enrolments']]
    df_snap.columns = ['quiz_name', 'medium','total_enrolments']
    obj.upload_file(df_snap, 'totalenrolmentsncert-event.data.csv')

def certificate_issued():
    df_snap = df_data[['Collection Name', 'Medium','Certificate Issued (100% completion)']]
    df_snap.columns = ['quiz_name', 'medium','certificate_issued_100_perc_completion']
    obj.upload_file(df_snap, 'certificateissued-event.data.csv')


def completion_perc():
    df_snap = df_data[['Collection Name', 'Medium', 'Completion %']]
    df_snap.columns = ['quiz_name', 'medium', 'completion_perc']
    obj.upload_file(df_snap, 'completionperc-event.data.csv')

def medium_dimension():
    df_snap = df_data[['Medium']].drop_duplicates()
    df_snap['medium_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['medium_id', 'Medium']]
    df_snap.columns = ['medium_id', 'medium']
    obj.upload_file(df_snap, 'medium-dimension.data.csv')

if df_data is not None:
    total_enrolments()
    certificate_issued()
    completion_perc()
    medium_dimension()
