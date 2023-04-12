from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()


def category_event_data():
    df_melt = df_data.melt(id_vars=['State Code'],
                           value_vars=["Mobile Survey Completed (Target: 3000 per State/ UT)","Number of DCR to be uploaded (Target)",
                                       "DCR Completed/ Uploaded (1498/1568)","State Position Paper e-template submitted","National District Groups (NDGs) created",
                                       "National DCR Submitted","National DCR Target","Total SSC Onboarded"],
                           var_name="category_name", value_name="category_value")
    df_snap = df_melt[['State Code','category_name', 'category_value']]
    df_snap.columns = ['state_id','category_name', 'category_value']
    obj.upload_file(df_snap, 'categoryncf-event.data.csv')

if df_data is not None:
    category_event_data()

