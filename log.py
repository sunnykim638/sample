import matplotlib.pyplot as plt
import pandas as pd

c = {'INITIAL_BUFFERING': 'violet', 'PLAY': 'lightcyan', 'BUFFERING': 'lightpink'}

dash = pd.read_csv("./ASTREAM_LOGS/DASH_BUFFER_LOG_2025-11-01.17_20_01.csv")
dash = dash.loc[dash.CurrentPlaybackState.isin(c.keys() )]
states = pd.DataFrame({'startState': dash.CurrentPlaybackState[0:-2].values, 'startTime': dash.EpochTime[0:-2].values,
                        'endState':  dash.CurrentPlaybackState[1:-1].values, 'endTime':   dash.EpochTime[1:-1].values})


for index, s in states.iterrows():
  plt.axvspan(s['startTime'], s['endTime'],  color=c[s['startState']], alpha=1)

plt.plot(dash[dash.Action!="Writing"].EpochTime, dash[dash.Action!="Writing"].CurrentBufferSize, 'kx:')
plt.title("Buffer(segments)");
plt.xlabel("Time (s)");


plt.savefig("buffer_plot.png", dpi=300, bbox_inches='tight')
plt.close()
