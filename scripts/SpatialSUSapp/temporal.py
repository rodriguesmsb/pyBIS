#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Tue Mar 01 2021
@author: Moreno rodrigues rodriguesmsb@gmail.com
"""

import dash
import os
import dash_core_components as dcc
import dash_table as dt
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_leaflet as dl
from dash_leaflet import express as dlx
from dash.dependencies import Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from aux.functions import functions
import json
import numpy as np



### Indicates path
path_to_data = "data/data.csv"
path_to_json = "conf/conf.json"
path_to_images = "assets/"


### Manipulate data
conf = functions(conf_file = path_to_json, data = path_to_data)


### Create a instance of Dash class
app = dash.Dash(__name__, 
external_stylesheets = ["https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css",
                        "https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500&display=swap"])
app.title = "Data visualization"

app.layout = html.Div(
    children = [
        html.Div(
            children = ["header"],
            className = "time-series-header-container"
        ),
        html.Div(
            children = ["Filters"],
            className = "time-series-filter-container"
        ),
        html.Div(
            children = ["hline"],
            className = "time-series-hline-container"
        ),
        html.Div(
            children = [
                html.Div(
                    children = [],
                    className = "time-series-body-content"
                )
            ],
            className = "time-series-body-container"
        ),
        html.Div(
            children = ["Footer"],
            className = "time-series-footer-container"
        )
    ],
    className = "time-series-container"
)


if __name__ == '__main__':
    app.run_server(debug=True)