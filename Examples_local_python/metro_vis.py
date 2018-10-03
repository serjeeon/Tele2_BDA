
import pandas as pd
import numpy as np

import colorsys
import networkx as nx
from bokeh.resources import CDN
from bokeh.io import output_notebook
output_notebook(resources=CDN)

pd.set_option('max_colwidth', 200)
pd.set_option('max_rows', 500)
from bokeh.plotting import figure, output_file, show, ColumnDataSource, save

from bokeh.io import show
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure


def _pseudocolor(val):
    """Convert number to color"""
    h = (1.0 - val) * 120 / 360
    r, g, b = colorsys.hsv_to_rgb(h, 1., 1.)
    return r * 255, g * 255, b * 255


def _rgb2hex(color):
    """Converts a list or tuple of color to an RGB string

    Args:
        color (list|tuple): the list or tuple of integers (e.g. (127, 127, 127))

    Returns:
        str:  the rgb string
    """
    return f"#{''.join(f'{hex(c)[2:].upper():0>2}' for c in color)}"


def _prepare_data(connections, stations, lines, data, radius_feature, color_feature, features):
    """
    Prepares data for plotting, creates objects necessary for bokeh.

    Parameters:
    -----------
    connections, stations, lines - data from metro class
    data : pd.DataFrame()
        data prepared by metro class
    radius_feature : str
        feature name to use as radius scaling
    color_feature : str
        feature name to use as color for nodes
    features : list of strings
        list of column names to show on graph

    Returns:
    -------
    source : ColumnDataSource with data
    TOOLTIPS : Tooltips for plot
    graph : networkx graph
    locations : dictionary of stations and normalized coordinates
    """
    # create graph
    stations['node_name'] = stations['name'] + '_' + stations['line'].astype(str)
    connections['time'] = 1
    graph = nx.Graph()

    for connection_id, connection in connections.iterrows():
        ind1 = connection['station1'] - 1
        ind2 = connection['station2'] - 1
        station1_name = stations.iloc[ind1]['node_name']
        station2_name = stations.iloc[ind2]['node_name']
        graph.add_edge(station1_name, station2_name, time=connection['time'])

    normed = stations[['longitude', 'latitude']]
    normed = normed - normed.min()
    normed = normed / normed.max()
    locations = dict(zip(stations['node_name'], normed[['longitude', 'latitude']].values))

    x = []
    y = []
    name = []
    radius = []
    fill_color = []
    line_l = []
    d = {i: [] for i in features}

    for node in graph.nodes():
        # main values
        x.append(locations[node][0])
        y.append(locations[node][1])
        name.append(node.split('_')[0])
        radius.append(np.clip(.01 * data.loc[(data['display_name'] == node.split('_')[0]) & (
                data['line'] == int(node.split('_')[1])), radius_feature].values[0], 0.003, 1))
        colour = _pseudocolor(data.loc[(data['display_name'] == node.split('_')[0]) & (
                data['line'] == int(node.split('_')[1])), color_feature].values[0])
        fill_color.append(_rgb2hex(tuple([int(np.round(i)) for i in colour])))
        line_l.append(lines.loc[lines.line == int(node.split('_')[1]), 'name'].values[0])

        # user defined values
        for k in d.keys():
            d[k].append(str(np.round(data.loc[(data['display_name'] == node.split('_')[0]) & (
                    data['line'] == int(node.split('_')[1])), 'total_traffic'].values[0], 2)))

    source = ColumnDataSource(data=dict(
        x=x,
        y=y,
        radius=radius,
        fill_color=fill_color,
        name=name,
        line_l=line_l
    ))
    for k, v in d.items():
        source.add(v, k)

    TOOLTIPS = []
    for i in features:
        TOOLTIPS.append((i, f'@{i}'))

    return source, TOOLTIPS, graph, locations


def plot(connections, stations, lines, data, radius_feature, color_feature, features, output_file_name=None,
         return_plot=False):
    """
    Docs in work
    :param connections:
    :param stations:
    :param lines:
    :param data:
    :param radius_feature:
    :param color_feature:
    :param features:
    :param output_file_name:
    :param return_plot:
    :return:
    """
    source, TOOLTIPS, graph, locations = _prepare_data(connections, stations, lines, data, radius_feature,
                                                       color_feature, features)
    p = figure(
        x_range=(.4, .7),
        y_range=(.2, .5),
        height=700,
        width=900, tooltips=TOOLTIPS
    )
    for edge in graph.edges():
        a = edge[0].split('_')[1]
        b = edge[1].split('_')[1]
        if a != b:
            line_color = '#FFFFFF'
            name = 'Пересадка'
        else:
            line_color = lines.loc[lines['line'] == int(a), 'colour'].values[0]
            name = lines.loc[lines['line'] == int(a), 'name'].values[0]

        p.line(
            x=[locations[pt][0] for pt in edge],
            y=[locations[pt][1] for pt in edge],
            line_color=line_color,
            line_width=5,
            name=name
        )

    p.circle(
        "x", "y", radius='radius',
        fill_color='fill_color',
        line_alpha=0, source=source)
    for node in graph.nodes():
        p.text(
            [locations[node][0]],
            [locations[node][1]],
            text={'value': node.split('_')[0]},
            text_font_size=str(max(data.loc[(data['display_name'] == node.split('_')[0]) & (
                        data['line'] == int(node.split('_')[1])), 'scaled_user_count'].values[0] * 1, 8)) + "pt",
            # text_alpha = pageranks[node],
            text_align='center',
            text_font_style='bold')
    if output_file_name:
        output_file(f"{output_file_name}.html")
        save(p)

    if return_plot:
        return p


if __name__ == '__main__':
    # create class instance and get data from api
    from metro import MetroData

    metro_data = MetroData(no_process=True)
    sql = """
            select * from UAT_DM.al_metro_subs_august where report_month = '2018-06-01'
    """
    # load data from Teradata
    metro_data.load_teradata_data(load_local=False, use_sql=True, sql=sql)
    # combine and save
    metro_data.combine_files(result_file_name='new_file.csv')
    lines = metro_data.lines
    stations = metro_data.stations
    connections = metro_data.connections
    data = metro_data.new_stations

    data['total_traffic'] = data['total_traffic'] / (1024 ** 3)
    data['scaled_user_count'] = (data['user_count'] / np.max(data['user_count'])).fillna(0)
    data['scaled_total_traffic'] = (data['total_traffic'] / np.max(data['total_traffic'])).fillna(0)
    plot(connections, stations, lines, data, 'scaled_user_count', 'scaled_total_traffic',
         features=['user_count', 'total_traffic'],
         output_file_name='metro_plot')
