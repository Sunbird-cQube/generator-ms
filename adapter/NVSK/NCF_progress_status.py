from main import CollectData

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
    df_snap.update(df_snap[['category_name']].applymap("'{}'".format))
    return df_snap
def category_dimension_data():
    df_data = category_event_data()
    obj.upload_file(df_data, 'categoryncf-event.data.csv')
    df_data = df_data[['category_name']].drop_duplicates()
    df_data['category_id'] = range(1, len(df_data) + 1)
    df_snap = df_data[['category_id', 'category_name']]
    df_snap.update(df_snap[['category_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'categoryncf-dimension.data.csv')

if df_data is not None:
    category_event_data()
    category_dimension_data()

