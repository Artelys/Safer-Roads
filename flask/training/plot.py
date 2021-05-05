# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 11:30:53 2020

@author: aboutet
"""

import matplotlib.pyplot as plt
from matplotlib import cm
import plotly.offline as off
import plotly.graph_objs as go
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


# Pyplot basic pie chart
def plot_repartition(df, colVal, colLab, dropna=False, figsize=(10, 10), cmap='rainbow', explode=None):
    data = df.copy()

    if dropna:
        data = data[(data[colLab] != "nan") & (data[colLab] != "NAN") & (data[colLab].notnull())]

    data[colLab] = data[colLab].astype('str').astype('category')
    data[colVal] = data[colVal].astype('str').astype('category')

    data = data[[colVal, colLab]].groupby(colLab).count()

    data = data.sort_values(by=colVal, ascending=False)

    if explode is not None:
        lasttoexplode = explode
        explode = [0 for i in range(len(data))]
        N = len(explode)
        ex = 0.2
        for i in range(lasttoexplode):
            explode[N - 1 - i] = ex
            ex = ex + 0.2

    data.plot.pie(y=colVal, cmap='rainbow', figsize=figsize, legend=False, explode=explode)



def homemade_piechart(labels, values, columnName, title):
    fig = {
      "data": [
        {
          "labels": labels,
          "values": values.AccidentID,
          #"domain": {"x": [0, 1]},
          "name": columnName,
          "hoverinfo":"label+percent+name",
          "hole": .6,
          "type": "pie"
        },
         ],
      "layout": {
            "title":title,
            "annotations": [
                {
                    "font": {
                        "size": 40
                    },
                    "showarrow": False,
                    "text": " ",
                    "x": 5.50,
                    "y": 0.5
                }
            ]
        }
    }
    plt.iplot(fig)

def violin_plotly(df, xname, yname, xmodalites, ylim=None):
    """Violin plot based on the plotly violin plot"""

    data = []
    for i in range(0, len(xmodalites)):
        trace = go.Violin(
            x=df[xname][df[xname] == xmodalites[i]],
            y=df[yname][df[xname] == xmodalites[i]],
            name=str(xmodalites[i]),
            box=dict(visible=True),
            meanline=dict(visible=True),
        )
        data.append(trace)

    layout = go.Layout(
        title="",
        yaxis=dict(
            zeroline=False,
            range=ylim
        )
    )

    fig = dict(data=data, layout=layout)
    off.iplot(fig)


def homemade_scatter(df, xname, yname, xlabel, ylabel, title, fit_reg=False,
                     confidence_interval=None, divide_x=None, divide_y=None,
                     colored_index=None, index_rename=None, title_fontsize=14,
                     ax=None, alpha=1, height=10, marker_size = 1):
    """Scatter plot, based either on seaborn lmplot if fit_reg is True
    or on seaborn scatterplot. In that case, a matplotlib.Axes instance
    can be passed.
    Can be scaled through divide_x and divide_y args.
    """
    tempo = df.copy()
    if divide_x != None:
        tempo[xname] = tempo[xname] / divide_x
    if divide_y != None:
        tempo[yname] = tempo[yname] / divide_y
    if index_rename != None:
        tempo.columns = tempo.rename({colored_index: index_rename}, axis='columns').columns
        colored_index = index_rename
    if fit_reg:
        g = sns.lmplot(xname, yname, tempo, hue=colored_index,
                       fit_reg=fit_reg, ci=confidence_interval, height=height, s = marker_size)
        g = g.set_axis_labels(xlabel, ylabel)
    else:
        if ax is None:
            ax = plt.subplots(figsize=(12, 8))[1]
        g = sns.scatterplot(x=xname, y=yname, data=tempo, hue=colored_index,
                            alpha=alpha, ax=ax, s = marker_size)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
    plt.title(title, fontsize=title_fontsize)


def add_color(go_bar, new_color=None):
    """util function to modify the color of plotly bar plot"""
    if new_color is not None:
        marker = go_bar['marker']
        marker.update(color=new_color)
        go_bar['marker'] = marker
    return go_bar


def barchart_plotly(df, xlabel, ynames, title, unit, ylabels, xname="index",
                    type="stack", width=1000, height=500,
                    divide=None, colors=None, ylim=None, text=None):
    """Plotly barchart. Can be rescale with the divide arg."""

    if colors is None:
        colors = [None] * len(ynames)

    data = []
    tempo = df.copy()

    if divide != None:
        for yname in ynames:
            tempo[yname] = tempo[yname] / divide

    if xname == "index":
        x_series = tempo.index
    else:
        x_series = tempo[xname]

    annotations = []
    y_offset = pd.Series(data=[0] * len(x_series), index=x_series)
    for i, yname in enumerate(ynames):
        trace = go.Bar(
            x=x_series,
            y=tempo[yname],
            name=ylabels[i],
        )
        trace = add_color(trace, colors[i])
        data.append(trace)

        # Add some text to the bars
        if i > 0:
            for j, (x, y) in enumerate(zip(x_series, tempo[yname])):
                y_txt = y
                y = y / 2 + y_offset.iloc[j]
                annotations.append(dict(x=x, y=y, text='{:.2f}TWh'.format(y_txt),
                                        font=dict(family='Arial', size=14,
                                                  color='rgba(0, 0, 0, 1)'),
                                        showarrow=False, yshift=0))

        y_offset = y_offset + tempo[yname]

    layout = go.Layout(
        yaxis=dict(
            range=ylim,
            title=unit,
            gridcolor='#ffffff',  # white
            zeroline=False
        ),
        xaxis=dict(
            title=xlabel,
            gridcolor='#ffffff',  # white
        ),
        title=title,
        barmode=type,
        width=width,
        height=height,
        autosize=True,
        paper_bgcolor='rgba(234, 234, 242, 1)',  # seaborn blue
        plot_bgcolor='rgba(234, 234, 242, 1)',  # seaborn blue
    )

    fig = dict(data=data, layout=layout)

    if text:
        fig["layout"].update(annotations=annotations)
		
	# Plot
    off.iplot(fig)


def matplotly(data, xname, ynames, title, xlabel="", ylabel="",
              mode="markers", ylim=[-1, 1], legend=None,
              opacity=None, width=1000, height=500,
              divide=None, colors=None, secondary_axis=None, ylabel2=None):
    """Customized Plotly scatter object. Can be rescaled through the divide
    arg.
    If you want an y axis, enter list with 1 and 2 to define which curve has to be on first and secondary axis"""

    tempo = data.copy()

    if divide != None:
        for yname in ynames:
            tempo[yname] = tempo[yname] / divide

    if legend is None:
        legend = ynames

    if colors is None:
        colors = [None] * len(ynames)

    if opacity is None:
        opacity = [1 for i in ynames]

    # Define x from index if required
    if xname is None or xname == "index":
        x = tempo.index
        mode = 'lines'
    else:
        x = tempo[xname]

    # Build the various curves for your plot
    traces = []
    for i, yname in enumerate(ynames):
        if secondary_axis != None:
            if secondary_axis[i] == 2:
                trace = go.Scattergl(
                    x=x,
                    y=tempo[yname],
                    opacity=opacity[i],
                    name=legend[i],
                    mode=mode,
                    yaxis='y2'
                )
                trace = add_color(trace, colors[i])
                traces.append(trace)
            else:
                trace = go.Scattergl(
                    x=x,
                    y=tempo[yname],
                    opacity=opacity[i],
                    name=legend[i],
                    mode=mode
                )
                trace = add_color(trace, colors[i])
                traces.append(trace)
        else:
            trace = go.Scattergl(
                x=x,
                y=tempo[yname],
                opacity=opacity[i],
                name=legend[i],
                mode=mode
            )
            trace = add_color(trace, colors[i])
            traces.append(trace)

    # Layout setting
    if secondary_axis == None:
        layout = go.Layout(
            yaxis=dict(
                range=ylim,
                title=ylabel,
                gridcolor='#ffffff'),
            xaxis=dict(
                title=xlabel,
                gridcolor='#ffffff',
                zeroline=True,
                zerolinecolor='#ffffff'),
            title=title,
            width=width,
            height=height,
            autosize=True,
            paper_bgcolor='rgba(234, 234, 242, 1)',
            plot_bgcolor='rgba(234, 234, 242, 1)'
        )
    else:
        layout = go.Layout(
            yaxis=dict(
                range=ylim,
                title=ylabel,
                gridcolor='#ffffff'),
            yaxis2=dict(
                range=ylim,
                title=ylabel2,
                gridcolor='#ffffff',
                side='right'),
            xaxis=dict(
                title=xlabel,
                gridcolor='#ffffff',
                zeroline=True,
                zerolinecolor='#ffffff'),
            title=title,
            width=width,
            height=height,
            autosize=True,
            paper_bgcolor='rgba(234, 234, 242, 1)',
            plot_bgcolor='rgba(234, 234, 242, 1)'
        )

    fig = dict(data=traces, layout=layout)

    # Plot
    off.iplot(fig)

def multiple_matplotly(data, xname, ynames, title, xlabel="", ylabel="",
                       mode="markers", ylim=[-1, 1], legend=None,
                       opacity=1, width=1000, height=500,
                       divide=None, colors=None):
    """Customized Multiple Plotly scatter object. Can be rescaled through the divide
    arg."""

    tempo = data.copy()

    if divide != None:
        for yname in ynames:
            tempo[yname] = tempo[yname] / divide

    if legend is None:
        legend = ynames

    if colors is None:
        colors = [None] * len(ynames)

    # Define x from index if required
    if xname is None or xname == "index":
        x = tempo.index
        mode = 'lines'
    else:
        x = tempo[xname]

    # Build the various curves for your plot
    traces = []
    for i, yname in enumerate(ynames):
        trace = go.Scattergl(
            x=x,
            y=tempo[yname],
            opacity=opacity,
            name=legend[i],
            mode=mode
        )
        trace = add_color(trace, colors[i])
        traces.append(trace)

    # Simply return traces to add at multiple plot
    return traces
