from main import CollectData

obj=CollectData()
program=obj.program
df_data = obj.get_file()


def onboarded_event_data():
    df_snap = df_data[['State Code', 'Onboarded on PM Poshan']]
    df_snap.columns = ['state_id', 'onboarded']
    # obj.get_file(df_snap, 'onboarded-event.data.csv')
    obj.upload_file(df_snap, 'onboarded-event.data.csv')


if df_data is not None:
    onboarded_event_data()
