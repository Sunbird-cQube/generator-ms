from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def schools_event_data():
    df_snap = df_data[['state_code','total_schools']]
    df_snap.columns=['state_id','total_schools']
    obj.upload_file(df_snap, 'schools-event.data.csv')

def total_boys_event():
    df_snap = df_data[['state_code','total_boys_in_schools']]
    df_snap.columns=['state_id','total_boys_in_schools']
    obj.upload_file(df_snap, 'totalboys-event.data.csv')

def total_girls_event():
    df_snap = df_data[['state_code','total_girls_in_schools']]
    df_snap.columns=['state_id','total_girls_in_schools']
    obj.upload_file(df_snap, 'totalgirls-event.data.csv')


def category_event_data():
    df_melt = df_data.melt(id_vars=['state_code'],
                           value_vars=[ "total_instructional_classrooms",
                            "total_schools_having_library",
                            "total_schools_having_handwash_facility",
                            "total_schools_having_drinking_water_facility",
                            "total_schools_having_ramp_facility",
                            "total_schools_having_playground",
                            "total_schools_having_internet_facility",
                            "only_boys_school",
                            "only_girls_school",
                            "co-ed_school"
                        ],
                           var_name="category_name", value_name="category_value")
    df_snap = df_melt[['state_code','category_name', 'category_value']]
    df_snap.columns = ['state_id','category_name', 'category_value']
    df_snap.update(df_snap[['category_name']].applymap("'{}'".format))
    return df_snap

def category_dimension_data():
    df_data=category_event_data()
    obj.upload_file(df_data, 'pm_shri_category-event.data.csv')
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    df_snap.update(df_snap[['category_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'categorypmshri-dimension.data.csv')

if df_data is not None:
    schools_event_data()
    total_boys_event()
    total_girls_event()
    category_event_data()
    category_dimension_data()
