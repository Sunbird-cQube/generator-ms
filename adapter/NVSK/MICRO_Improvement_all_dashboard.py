from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()


def participating_program_status():
    df_snap = df_data[['State Code', 'Started Micro-Improvement']]
    df_snap.columns = ['state_id', 'participating_in_micro_improvement_program_status']
    obj.upload_file(df_snap, 'participatingprogramstatus-event.data.csv')

if df_data is not None:
    participating_program_status()