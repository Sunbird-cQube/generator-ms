from main import CollectData

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def teachersmarked():
    df_snap = df_data[['date','district_id','block_id','cluster_id','school_id','schoolcategory_name','schoolmanagement_name','school_statecategory_name','grade','teachers_marked']].drop_duplicates()
    df_snap.columns=['date','district_id','block_id','cluster_id','school_id','schoolcategory_name','schoolmanagement_name','school_statecategory_name','grade','teachers_marked']
    obj.upload_file(df_snap, 'teachersmarked-event.data.csv')

def teacherspresent():
    df_snap = df_data[['date','district_id','block_id','cluster_id','school_id','schoolcategory_name','schoolmanagement_name','school_statecategory_name','grade','teachers_marked_present']].drop_duplicates()
    df_snap.columns=['date','district_id','block_id','cluster_id','school_id','schoolcategory_name','schoolmanagement_name','school_statecategory_name','grade','teachers_marked_present']
    obj.upload_file(df_snap, 'teacherspresent-event.data.csv')

def totalteachers():
    df_snap = df_data[['date','district_id','block_id','cluster_id','school_id','schoolcategory_name','schoolmanagement_name','school_statecategory_name','grade','total_teachers']].drop_duplicates()
    df_snap.columns=['date','district_id','block_id','cluster_id','school_id','schoolcategory_name','schoolmanagement_name','school_statecategory_name','grade','total_teachers']
    obj.upload_file(df_snap, 'totalteachers-event.data.csv')

if df_data is not None:
    teachersmarked()
    teacherspresent()
    totalteachers()