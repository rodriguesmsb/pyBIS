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



#path_to_data = "scripts/SpatialSUSapp/data/data.csv"
#path_to_json =  "scripts/SpatialSUSapp/conf/conf.json"

path_to_data = "data/data.csv"
path_to_json =  "conf/conf.json"

conf = functions(conf_file = path_to_json, data = path_to_data)


###Add code to use external css

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

if __name__ == '__main__':
    app.run_server(debug = True)
