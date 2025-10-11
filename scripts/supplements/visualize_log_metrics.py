import os

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
                        vertical_spacing=.1,
                        subplot_titles=['<b>Time Elapsed</b>', 
                                        '<b>Success Rate</b>', 
                                        '<b>Pulls Per Second</b>', 
                                        '<b>Successful Pulls</b>'])
    
    col_num = 1
    row_num = 1
    for col in ['time_elapsed', 
                'success_rate', 
                'pulls_per_sec', 
                'successful_pulls']:
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
                             'symbol': 'circle',
                             'opacity': .75
                         },
                         line={
                             'width': .25
                         })
        fig.add_trace(trc, 
                      row=row_num, 
                      col=col_num)
        
        col_num += 1
    
    fig.update_xaxes(showspikes=True, spikethickness=.25, spikedash='solid')
    fig.update_yaxes(showspikes=True, spikethickness=.25, spikedash='solid')
    fig.update_layout(showlegend=False,
                      title={'text': plot_title,
                             'subtitle': {'text': notes}})
    
    fig.write_html(out_path)
    

def main():
    pio.templates.default = 'plotly_dark'

    SUMMARY_CFG = {
        'alx_ad_infinitum': {
            'title': '<b>Log Metrics for alx_ad_infinitum</b> (semi-recursive book ID search)',
            'notes': 'placeholder'
        },
        'pnd2alx': {
            'title': '<b>Log Metrics for pnd2alx</b> (using "pound.book_sample" column to fill "alexandria")',
            'notes': 'placeholder'
        },
        'sim_authors': {
            'title': '<b>Log Metrics for sim_authors</b> (filling "pound.sim_authors")',
            'notes': 'placeholder'
        },
        'sim_books': {
            'title': '<b>Log Metrics for sim_books</b> (filling "alexandria.sim_books")',
            'notes': 'placeholder'
        },
        'sitemap2pound': {
            'title': '<b>Log Metrics for sitemap2pound</b> (using the public sitemap to fill "pound")',
            'notes': 'placeholder'
        },
        'sitemap2dmitry': {
            'title': '<b>Log Metrics for sitemap2dmitry</b> (using the public sitemap to fill "false_dmitry")',
            'notes': 'placeholder'
        }
    }

    SUMMARY_PATH = os.path.join('logs', 'out_summary')
    OUT_PATH = os.path.join('docs', 'log_metrics')

    for subjfile in os.listdir(SUMMARY_PATH):

        subj_name = subjfile.replace('_SUMMARY.csv', '')
        subj_title = SUMMARY_CFG[subj_name]['title']
        subj_notes = SUMMARY_CFG[subj_name]['notes']
        
        full_path = os.path.join(SUMMARY_PATH, subjfile)
        subj_out_path = os.path.join(OUT_PATH, f'{subj_name}_metrics.html')

        viz_and_out(log_summary_path=full_path,
                    plot_title=subj_title,
                    notes=subj_notes,
                    out_path=subj_out_path)


if __name__ == '__main__':
    main()

