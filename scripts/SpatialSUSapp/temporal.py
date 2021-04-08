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
from datetime import date



### Indicates path
path_to_data = "scripts/SpatialSUSapp/data/data.csv"
path_to_json = "scripts/SpatialSUSapp/conf/conf.json"
path_to_images = "scripts/SpatialSUSapp/assets/"


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
            children = [
                html.H1(
                        conf.return_title(),
                        className = "temporal-header-title"
                        ),
            ],
            className = "temporal-header",
        ),
        html.Div(
            children = [],
            className = "time-series-footer-container"
        ),
        html.Div(
            children = [
                html.Label(
                    ["Selecione o intervalo de tempo",
                    dcc.DatePickerRange(
                        id = "data_picker",
                        start_date = date(2010,1,1),
                        end_date_placeholder_text = 'MM/DD/YYYY',
                        clearable = True,
                        with_portal = True,
                        
                )],
                className = "date-picker"),
                html.Div(
                    children = [
                        html.Label(
                    ["Filtrar por grupos",
                    dcc.Dropdown(
                        id = "city_picker",
                        searchable = True,

                        placeholder = "Selecione um municipio",
                    )],
                ),
                dcc.Dropdown(
                        id = "var_picker",
                        searchable = True,
                        clearable = True,
                        placeholder = "Selecione uma variável",
                    )
                    ],
                    className = "dropdown-selectors"
                )

            ],
            className = "filter"
        ),
        html.Div(
            children = ["hline"],
            className = "time-series-hline-container"
        ),
        html.Div(
            children = [
                html.Div(
                    children = [
                        html.Div(
                            [dcc.Graph(id = "daily_series")],
                            className = "daily-series"
                        ),
                        html.Div(
                            [dcc.Graph(id = "weekly_series")],
                            className = "weekly-series"
                        ),
                        html.Div(
                            [dcc.Graph(id = "monthly-series")],
                            className = "monthly-series"
                        ),
                        html.Div(
                            [dcc.Graph(id = "heat_map")],
                            className = "heat-series"
                        ),
                        html.Div(
                            [dcc.Graph(id = "monthly_grouped")],
                            className = "monthly-grouped"
                        )
                        
                    ],
                    className = "time-series-body-content"
                )
            ],
            className = "time-series-body-container"
        )
    ],
    className = "time-series-container"
)


if __name__ == '__main__':
    app.run_server(debug=True)