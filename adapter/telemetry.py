from adapter.VSK.main import CollectData

obj = CollectData()
program = obj.program
df_data = obj.get_file()


def browsers_event():
    df_snap = df_data[['date', 'userId', 'browserType']]
    df_snap['browserCount'] = '1'
    df_snap.columns = ['date', 'userId', 'browserName', 'browserCount']
    obj.upload_file(df_snap, 'browsersCount-event.data.csv')


def browsers_dimension():
    df_snap = df_data[['browserType']].drop_duplicates()
    df_snap.columns = ['browserName']
    obj.upload_file(df_snap, 'browsers-dimension.data.csv')


def devices_dimension():
    df_snap = df_data[['deviceType']].drop_duplicates()
    df_snap.columns = ['deviceName']
    obj.upload_file(df_snap, 'devices-dimension.data.csv')


def pageEvents_dimension():
    df_snap = df_data[['pageEvent']].drop_duplicates()
    df_snap.columns = ['pageEvent']
    obj.upload_file(df_snap, 'pageEvents-dimension.data.csv')


def pageNames_dimension():
    df_snap = df_data[['pageName']].drop_duplicates()
    df_snap.columns = ['pageName']
    obj.upload_file(df_snap, 'pageNames-dimension.data.csv')


def users_dimension():
    df_snap = df_data[['userId']].drop_duplicates()
    df_snap.columns = ['userId']
    obj.upload_file(df_snap, 'users-dimension.data.csv')


def device_event():
    df_snap = df_data[['date', 'userId', 'deviceType']]
    df_snap['deviceCount'] = '1'
    df_snap.columns = ['date', 'userId', 'deviceName', 'deviceCount']
    obj.upload_file(df_snap, 'deviceCount-event.data.csv')


def pageEvents_event():
    df_snap = df_data[['date', 'userId', 'pageName', 'pageEvent']]
    df_snap['pageEventCount'] = '1'
    df_snap.columns = ['date', 'userId', 'pageName', 'pageEvent', 'pageEventCount']
    obj.upload_file(df_snap, 'pageEventCount-event.data.csv')


def pageNames_event():
    df_snap = df_data[['date', 'pageName', 'pageEvent']]
    df_snap['pageCount'] = '1'
    df_snap.columns = ['date', 'pageName', 'pageEvent', 'pageCount']
    obj.upload_file(df_snap, 'pageNameCount-event.data.csv')


def timeSpent_event():
    df_snap = df_data[['date', 'userId', 'pageName', 'timeSpent']]
    obj.upload_file(df_snap, 'timeSpent-event.data.csv')


def userCount_event():
    df_snap = df_data[['date', 'userId']]
    df_snap['userCount'] = '1'
    df_snap.columns = ['date', 'userId', 'userCount']
    obj.upload_file(df_snap, 'userCount-event.data.csv')


if df_data is not None:
    browsers_event()
    browsers_dimension()
    device_event()
    devices_dimension()
    pageEvents_dimension()
    pageEvents_event()
    pageNames_event()
    pageNames_dimension()
    userCount_event()
    users_dimension()
    timeSpent_event()
