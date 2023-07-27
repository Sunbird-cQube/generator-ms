from main import CollectData

obj=CollectData()
program=obj.program
df_data = obj.get_file()

def total_meal_served():
    df_snap = df_data[['date','district_code', 'meal_served']]
    df_snap.columns = ['date','district_id', 'total_meals_served']
    obj.upload_file(df_snap, 'mealserved-event.data.csv')

def category_event_data():
    df_melt=df_data.melt(id_vars=['date','district_code'],
                     value_vars=['enrolled','total_schools'],
                     var_name="category_name",value_name="category_value")
    df_snap=df_melt[['date','district_code','category_name','category_value']]
    df_snap.columns=['date','district_id','category_name','category_value']
    df_snap.update(df_snap[['category_name']].applymap("'{}'".format))
    return df_snap

def category_dimenstion_data():
    df_data=category_event_data()
    obj.upload_file(df_data, 'category-event.data.csv')
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    df_snap.update(df_snap[['category_id']].applymap("'{}'".format))
    obj.upload_file(df_snap, 'categorypm-dimension.data.csv')

if df_data is not None:
    total_meal_served()
    category_event_data()
    category_dimenstion_data()
