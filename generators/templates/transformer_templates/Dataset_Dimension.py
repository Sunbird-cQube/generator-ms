import pandas as pd
from db_connection import *
import glob
file_list=glob.glob(os.path.dirname(root_path) + "processing_data/{KeyFile}")

con,cur=db_connection()

def Datainsert(valueCols={ValueCols}):
    df_data = pd.concat(pd.read_csv(file) for file in path)
    df_snap = df_data[valueCols]
    print(df_snap)
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in valueCols:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values});'''\
            .format(','.join(map(str,values)))
            print(query)
            cur.execute(query)
            con.commit()

    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if con is not None:
            con.close()

Datainsert()

















