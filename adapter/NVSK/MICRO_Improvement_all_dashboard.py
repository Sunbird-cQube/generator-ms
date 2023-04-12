from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()


def participating_program_status():
    df_snap = df_data[['State Code', 'Started Micro-Improvement']]
<<<<<<< HEAD
    df_snap.columns = ['state_id', 'participating_in_micro_improvement_program_status']
=======
    df_snap.columns = ['state_id', 'started']
>>>>>>> 24313b52ed73e717828542b88d2b1ff105402b2b
    obj.upload_file(df_snap, 'programstartedmicro-event.data.csv')

if df_data is not None:
    participating_program_status()