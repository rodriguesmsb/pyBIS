#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Tue Jan 26 2021
@author: Moreno rodrigues rodriguesmsb@gmail.com
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from app import app
from apps import spatio_temporal
from aux.functions import functions
import pandas as pd
import plotly.graph_objects as go

#### define function to hover on map
def get_info(feature=None):
    header = [html.H4("Municipio")]
    if not feature:
        return header + ["Hoover over a state"]
    return header + [html.B(feature["properties"]["name"]), html.Br()]
#"{:} people / mi".format(feature["properties"]["codmunres"]), html.Sup("2")


def get_id(feature = None):
    if not feature:
        return ["Selecione um municipio"]
    return int(str(feature["properties"]["id"][0:6]))


# path_to_data = "scripts/SpatialSUSapp/data/data.csv"
# path_to_json =  "scripts/SpatialSUSapp/conf/conf.json"

path_to_data = "data/data.csv"
path_to_json =  "conf/conf.json"

conf = functions(conf_file = path_to_json, data = path_to_data)

data = conf.read_data()

data = data.groupby([conf.return_area(), conf.return_time()]).size().reset_index(name = "count")


def plotTs(df):
    cases_trace = go.Scatter(
        x  = df[conf.return_time()],
        y =  df["count"],
        mode ='markers',
        name = "Fitted",
        line = {"color": "#d73027"}
    )
    data = [cases_trace]
    layout = go.Layout(yaxis = {"title": "Incidência"})
    return {"data": data, "layout": layout}

###Create a instance of Dash class
app = dash.Dash(__name__, 
external_stylesheets = ["https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css",
                        "https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500&display=swap"])
app.title = "Data visualization"


app.layout = html.Div([
    dcc.Location(id = 'url', refresh = False),
    html.Div(id = 'page-content')
])




#define all calllback that will be used
@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    pathname = conf.set_pathname()
    if pathname == "spatio_temporal":
        return spatio_temporal.layout
    elif pathname == "temporal":
        return 404

@app.callback(Output("info", "children"), [Input("geojson", "hover_feature")])
def info_hover(feature):
    return get_info(feature)


@app.callback(Output(component_id = "time-series-cases", component_property = "figure"),
              [Input(component_id = "geojson", component_property = "hover_feature")])
def update_Graph(feature):
    filtered_df = data[data["ID_MN_RESI"] == get_id(feature)]
    return plotTs(df = filtered_df)


@app.callback([Output(component_id = "data_table", component_property = "data")],
               Input(component_id = "geojson", component_property = "hover_feature"))
def update_table(feature):
    filtered_df = data[data["ID_MN_RESI"] == get_id(feature)]
    summary = filtered_df["count"].describe().reset_index()[1:]
    summary = summary.rename(columns = {"index": " "})
    print(summary)
    return [summary.to_dict("records")]


if __name__ == '__main__':
    app.run_server(debug = True)
