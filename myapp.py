import pandas as pd
import numpy as np
from fbprophet import Prophet
import matplotlib.pyplot as plt
from pyhive import hive
import bokeh
from bokeh.plotting import figure, output_file, show, curdoc
from bokeh.models import HoverTool, NumeralTickFormatter, SingleIntervalTicker, LinearAxis, TickFormatter, String, Int, ColumnDataSource, Span
from bokeh.layouts import row, column
from bokeh.models.widgets import PreText, Select

# wczytanie danych z hive za pomoca usera technicznego
#enter user and password
PWD = 'password'
user = "user"
port2 = 'port'
QUERY = 'select * from table'
QUEUE_QUERY = 'set mapred.job.queue.name=KOLEJKA'
server = 'serwer'
cursor = hive.connect(server, port = port2, username = user, auth='LDAP', password = PWD).cursor()
cursor.execute(QUEUE_QUERY)
cursor.execute(QUERY)

# zapisanie list z danymi do wykresu
lista_x = []
lista_vals = []
for i in cursor.fetchall():
    lista_x.append(i[0])
    lista_vals.append(i[1])

df = pd.DataFrame(
    {'ds': lista_x,
     'y': lista_vals
    })

df['ds'] = pd.to_datetime(df['ds'])
df['y'] = df['y'].astype(int)

# okreslenie cap i floor dla danych wejsciowych jest wymagane przy okreslaniu ich dla forecastu
df['cap'] = 300000
df['floor'] = 0

# dopasowanie modelu
m = Prophet(weekly_seasonality=True)
m.add_seasonality(name='monthly',
                  period=30.5,
                  fourier_order=7)
m.fit(df)

# prognoza
future = m.make_future_dataframe(periods=30)
future['cap'] = 300000
future['floor'] = 0
forecast = m.predict(future)

# filtrowanie danych forecastowych (to tylko do sumy jest wlasciwie potrzebne)
# forecast_filtered = forecast[forecast['ds'] >= '2018-03-14']
# forecast_filtered_2 = forecast_filtered[forecast_filtered['ds'] <= '2018-03-31']
forecast_filtered_2 = forecast[forecast['ds'] <= '2018-03-31']

# wykres bokeha
# output_file("chart_user_tech.html")

p = figure(plot_width=1200,
           plot_height=600,
           x_axis_type="datetime",
           y_range=(0, 350000)
           )
p.line(source = forecast_filtered_2,
       x = 'ds',
       y= 'yhat',
       line_width = 2,
       alpha=0.8,
       color = 'firebrick',
       )
# p.line(source = forecast_filtered_2,
#        x = 'ds',
#        y= 'yhat_lower',
#        line_width = 2,
#        alpha=0.2,
#        color = 'firebrick'
#        )
# p.line(source = forecast_filtered_2,
#        x = 'ds',
#        y= 'yhat_upper',
#        line_width = 2,
#        alpha=0.2,
#        color = 'firebrick'
#        )

# hovertool do podlgadania tooltipow i porownywania
cr = p.circle(source = forecast_filtered_2,
              x = 'ds',
              y= 'yhat',
              size=20,
              fill_color="grey",
              hover_fill_color="firebrick",
              fill_alpha=0.0,
              hover_alpha=0.3,
              line_color=None,
              hover_line_color="white"
              )
p.add_tools(HoverTool(tooltips=[("data", "@ds{%F}"),
                                ("GMV", "@yhat{0,0.00}")],
                      renderers=[cr],
                      mode='hline',
                      formatters={"ds": "datetime"}))

p.circle('ds', 'yhat', size=5, source=forecast_filtered_2[forecast_filtered_2['ds'] >= '2018-03-14'], color = 'firebrick')
p.circle('ds', 'y', size=5, source=df[df['ds'] < '2018-03-14'], color = 'black')


# linia na 300k
y = []
for i in range(len(forecast_filtered_2)):
    y.append(300000)
source = ColumnDataSource(dict(x = forecast_filtered_2['ds'],
                               y = y))

line = bokeh.models.glyphs.Line(x='x',
                                y='y',
                                line_color='red',
                                line_width=1,
                                line_alpha=0.5,
                                line_dash="6 4")
p.add_glyph(source, line)

# wstegi lower-upper
p.patch(np.append(forecast_filtered_2['ds'],
                  forecast_filtered_2['ds'][::-1]),
        np.append(forecast_filtered_2['yhat_lower'],
                  forecast_filtered_2['yhat_upper'][::-1]),
        color='firebrick',
        line_width=1,
        fill_alpha=0.1,
        line_alpha=0.3)

# trend liniowy GMV
p.line(source = forecast_filtered_2,
       x = 'ds',
       y= 'trend',
       line_width = 1,
       alpha=0.5,
       color = 'firebrick',
       line_dash="6 4"
       )

# ogarka
p.xgrid.grid_line_color = None
p.x_range.range_padding = 0.1
p.xaxis.major_label_orientation = 1
p.yaxis.formatter=NumeralTickFormatter(format="0,0")


stats = PreText(text='', height = 500, width=600)
prep = pd.DataFrame(forecast_filtered_2, columns = ['ds', 'yhat'])
stats.text = str(prep[prep['ds'] >= '2018-03-14'])

# main_row = row(p, stats)
# series = row(p, stats)
# layout = column(main_row, series)

curdoc().add_root(row(p, stats))
curdoc().title = "Prognoza GMV - Zestawy"

# show(p)

# # wydruk sum dla prognozy i zapis do pliku .csv
# print(forecast_filtered_2[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])
# forecast_filtered_2[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv('output_20180302.csv', sep=',')
# print('mid: ' + str(round(sum(forecast_filtered_2['yhat']) + 10329770)))
# print('low: ' + str(round(sum(forecast_filtered_2['yhat_lower']) + 10329770)))
# print('high: ' + str(round(sum(forecast_filtered_2['yhat_upper']) + 10329770)))

# # wykresy
# m.plot(forecast)
# m.plot_components(forecast)
# plt.show()

