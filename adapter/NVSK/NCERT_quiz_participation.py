from main import CollectData
import re

obj=CollectData()
program=obj.program
df_data=obj.get_file()

def quiz_name():
    df_snap = df_data[['State Code', 'Quiz Name']]
    df_snap.columns = ['state_id', 'quiz_name']
    obj.upload_file(df_snap, 'quizname-event.data.csv')


def total_enrollments():
    df_snap = df_data[['State Code', 'Total Enrollments']]
    df_snap.columns = ['state_id', 'total_enrollments']
    obj.upload_file(df_snap, 'totalenrollments-event.data.csv')


def quiz_dimension():
    df_snap = df_data[['Quiz Name']].drop_duplicates()
    df_snap['quiz_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['quiz_id', 'Quiz Name']]
    df_snap.columns = ['quiz_id', 'quiz_name']
    obj.upload_file(df_snap, 'quiz-dimension.data.csv')

def medium_dimension():
    df_snap = df_data[['Medium']].drop_duplicates()
    df_snap['medium_id'] = range(1, len(df_snap) + 1)
    df_snap = df_snap[['medium_id', 'Medium']]
    df_snap.columns = ['medium_id', 'medium']
    obj.upload_file(df_snap, 'medium-dimension.data.csv')

if df_data is not None:
    quiz_name()
    total_enrollments()
    quiz_dimension()
    medium_dimension()

