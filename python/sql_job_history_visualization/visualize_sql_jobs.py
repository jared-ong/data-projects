# -*- coding: utf-8 -*-
"""
Created on Tue Feb 19 22:35:01 2019

@author: jong
"""


import pandas as pd
import plotly
import plotly.figure_factory as ff

plotly.tools.set_credentials_file(username='jongmdsol', api_key='SJYqKTEN2c9nai8F07nk')

df = pd.read_csv('C:\\Users\\jong\\Documents\\GitHub\\data-projects\\python\\sql_job_history_visualization\\job_history_example3.csv', index_col=None)

#df1 = df[['name','job_id','run_status','run_start_time','run_end_time']]
#df1.rename(columns = {'name':'Task','job_id':'job_id','run_start_time':'Start','run_end_time':'Finish', 'run_status':'Complete'}, inplace = True)
#df1 = df.head(30)
df1 = df.head(100)


#df1 = [dict(Task="Job A", Start='2009-01-01', Finish='2009-02-28', Complete=10),
#      dict(Task="Job B", Start='2008-12-05', Finish='2009-04-15', Complete=60),
#      dict(Task="Job C", Start='2009-02-20', Finish='2009-05-30', Complete=95)]

df1['Start'] = pd.to_datetime(df1['Start'], errors='coerce')
df1['Finish'] = pd.to_datetime(df1['Finish'], errors='coerce')

#df1['Start'] = df1['Start'].dt.floor('T')
#df1['Finish'] = df1['Finish'].dt.floor('T')

fig = ff.create_gantt(df1, colors='Viridis', index_col='Complete', show_colorbar=False,
                      bar_width=0.2, showgrid_x=True, showgrid_y=True, width=1000, height=1000,
                      group_tasks=True, task_names=None)
plotly.plotly.iplot(fig, filename='gantt-sql-jobs', world_readable=True)



df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/gantt_example.csv')

fig = ff.create_gantt(df, colors=['#333F44', '#93e4c1'], index_col='Complete', show_colorbar=True,
                      bar_width=0.2, showgrid_x=True, showgrid_y=True)
plotly.plotly.iplot(fig, filename='gantt-use-a-pandas-dataframe', world_readable=True)


df = [dict(Task="Job A", Start='2009-01-01', Finish='2009-02-28'),
      dict(Task="Job B", Start='2009-03-05', Finish='2009-04-15'),
      dict(Task="Job C", Start='2009-02-20', Finish='2009-05-30')]

fig = ff.create_gantt(df)
plotly.plotly.iplot(fig, filename='gantt-simple-gantt-chart', world_readable=True)