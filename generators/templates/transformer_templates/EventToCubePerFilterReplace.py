import pandas as pd
from db_connection import *
import glob
file_list=glob.glob(os.path.dirname(root_path) + "processing_data/{KeyFile}")

con,cur=db_connection()

def filterTransformer(valueCols={ValueCols}):
    df_event = pd.concat(pd.read_csv(file) for file in path)
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}',con=con).drop_duplicates()  ### reading DimensionDataset from Database
    df_dimension.update(df_dimension[{DimColCast}].applymap("'{Values}'".format))
    event_dimension_merge = df_event.merge(df_dimension, on=['{MergeOnCol}'],how='inner')  ### mapping dataset with dimension
    df_total = event_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})  ### aggregation before filter
    df_total['{DenominatorCol}'] = df_total['{AggCol}'] ### renaming dataset columns
    df_filter = event_dimension_merge.loc[event_dimension_merge['{FilterCol}']{FilterType}{Filter}]  ### applying filter
    df_filter = df_filter.groupby({GroupBy}, as_index=False).agg({AggCols})  ### aggregation after filter
    df_filter['{NumeratorCol}'] = df_filter['{AggCol}'] ### renaming dataset columns
    df_agg = df_filter.merge(df_total, on={GroupBy}, how='inner')  ### merging aggregated DataFrames
    df_agg['percentage'] = ((df_agg['{NumeratorCol}'] / df_agg['{DenominatorCol}']) * 100)  ### Calculating Percentage
    df_snap = df_agg[valueCols]
    print(df_snap)
    try:
        for index, row in df_snap.iterrows():
            values = []
            for i in valueCols:
                values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {ReplaceFormat};''' \
                .format(','.join(map(str, values)))
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
filterTransformer()




