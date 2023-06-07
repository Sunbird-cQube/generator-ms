from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def students_event_data():
    df_snap = df_data[['state_code','district_code','number_of_students']]
    df_snap.columns=['state_id','district_id','no_of_students']
    obj.upload_file(df_snap, 'students-event.data.csv')

def category_event_data():
    df_melt = df_data.melt(id_vars=['district_code', 'state_code'],
                           value_vars=["ptr","%_schools_having_toilet","%_schools_having_drinking_water","%_schools_having_electricity","%_schools_having_library","%_govt_aided_schools_received_textbook","%_schools_with_ramp"],
                           var_name="category_name", value_name="category_value")
    df_snap = df_melt[['state_code', 'district_code', 'category_name', 'category_value']]
    df_snap.columns = ['state_id', 'district_id', 'category_name', 'category_value']
    df_snap.update(df_snap[['category_name']].applymap("'{}'".format))
    return df_snap

def category_dimenstion_data():
    df_data=category_event_data()
    obj.upload_file(df_data, 'category-event.data.csv')
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    df_snap.update(df_snap[['category_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'categoryudise-dimension.data.csv')

if df_data is not None:
    students_event_data()
    category_event_data()
    category_dimenstion_data()
