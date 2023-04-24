from main import CollectData

obj=CollectData()
program=obj.program
df_data = obj.get_file()
col_list = df_data.columns.tolist()
final_col_list=[]
date=None
month=None
for i in col_list:
    if 'MealServed' in i:
        slice=i.split('(')
        date=slice[1].strip(')')
        final_col_list.append(slice[0])
    elif 'Enrolled In' in i:
        slice = i.split(' ')
        month =slice[2]
        final_col_list.append(slice[0])
    else:
        final_col_list.append(i)
df_data = df_data[col_list]
df_data.columns = final_col_list
df_data['date']=date
df_data['month']=month

def total_meal_served():
    df_snap = df_data[['date','District Code', 'MealServed']]
    df_snap.columns = ['date','district_id', 'total_meals_served']
    obj.upload_file(df_snap, 'totalmealserved-event.data.csv')

def category_event_data():
    df_melt=df_data.melt(id_vars=['District Code','month'],
                     value_vars=['Enrolled','Total Schools'],
                     var_name="category_name",value_name="category_value")
    df_snap=df_melt[['month','District Code','category_name','category_value']]
    df_snap.columns=['month','district_id','category_name','category_value']
    return df_snap

def category_dimenstion_data():
    df_data=category_event_data()
    obj.upload_file(df_data, 'category-event.data.csv')
    df_data=df_data[['category_name']].drop_duplicates()
    df_data['category_id']= range(1, len(df_data) + 1)
    df_snap=df_data[['category_id','category_name']]
    obj.upload_file(df_snap, 'categorypm-dimension.data.csv')

if df_data is not None:
    total_meal_served()
    category_event_data()
    category_dimenstion_data()
