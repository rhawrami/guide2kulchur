import os
import time

import pandas as pd
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def logsummary2df(log_summary_path: str) -> pd.DataFrame:
    """return pandas dataframe of log summary data"""
    df = pd.read_csv(log_summary_path, dtype=str)
    for col in df.columns:
        if col in ['date', 'time']:
            continue
        else:
            if col in ['semaphore', 'batch_no', 'attempted']:
                df[col] = df[col].astype(int)
            else:
                df[col] = df[col].astype(float)                
    
    df['timestamp'] = df['date'] + ' ' + df['time']
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%m-%d-%Y %H:%M:%S')

    df['successful_pulls'] = (df['success_rate'] * df['attempted']).astype(int)

    return df
    

def viz_and_out(log_summary_path: str,
                plot_title: str,
                notes: str,
                out_path: str) -> None:
    """vizualize a summary df; write HTML"""
    df = logsummary2df(log_summary_path=log_summary_path)
    
    fig = make_subplots(rows=2, 
                        cols=2,
                        shared_xaxes=False,
                        horizontal_spacing=.05, 
                        vertical_spacing=.15,
                        subplot_titles=['<b>Time Elapsed</b>', '<b>Success Rate</b>', '<b>Pulls Per Second</b>', '<b>Successful Pulls</b>'])
    
    fig2 = go.Figure()
    
    col_num = 1
    row_num = 1
    for col in ['time_elapsed', 
                'success_rate', 
                'pulls_per_sec', 
                'successful_pulls']:
        print(row_num, col_num)
        if col_num > 2:
            col_num = 1
            row_num += 1
        
        df['txt'] = (
            '<b>' + df['timestamp'].astype(str) + '</b><br>' +
            '<b>Batch ID</b>: ' + df['batch_no'].astype(str) + '<br>' +
            '<b>Semaphore Count</b>: ' + df['semaphore'].astype(str) + '<br>' +
            '<b>Sub-Batch Delay</b>: ' + df['subbatch_delay'].astype(str) + '<br>' +
            '<b>Attempted</b>: ' + df['attempted'].astype(str) + '<br>' +
            '<b>Metric</b>: ' + df[col].astype(str) + '<br>'
        )
        
        trc = go.Scatter(name=col,
                         x=df['timestamp'],
                         y=df[col],
                         hoverinfo='text',
                         text=df['txt'],
                         mode='lines+markers',
                         marker={
                             'size': 3,
                             'color': 'white',
                             'symbol': 'diamond',
                             'opacity': .75
                         },
                         line={
                             'width': .25
                         })
        fig.add_trace(trc, 
                      row=row_num, 
                      col=col_num)
        
        fig2.add_trace(trc)
        
        col_num += 1
    
    fig2.update_xaxes(showspikes=True, spikethickness=.25, spikedash='solid')
    fig2.update_yaxes(showspikes=True, spikethickness=.25, spikedash='solid')
    fig2.update_layout(#showlegend=False,
                       title={'text': plot_title})
    
    fig2.show()
    

def main():
    pio.templates.default = 'plotly_dark'
    return None


if __name__ == '__main__':
    pio.templates.default = 'plotly_dark'
    viz_and_out(log_summary_path=os.path.join('logs','out_summary', 'sim_authors_SUMMARY.csv'),
                plot_title='<b>Log Metrics for pnd2alx</b>',
                notes='nteh',
                out_path='')

