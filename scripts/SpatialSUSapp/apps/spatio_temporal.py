#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Tue Jan 26 2021
@author: Moreno rodrigues rodriguesmsb@gmail.com
"""

import os
import dash_core_components as dcc
import dash_table as dt
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_leaflet as dl
from dash_leaflet import express as dlx
import plotly.express as px
import pandas as pd
from aux.functions import functions
import json


path_to_data = "scripts/SpatialSUSapp/data/data.csv"
path_to_json = "scripts/SpatialSUSapp/conf/conf.json"
path_to_images = "scripts/SpatialSUSapp/assets/"


conf = functions(conf_file = path_to_json, data = path_to_data)
json_map = path_to_images + "maps/geojs-" + conf.set_json_map() + "-mun.json"

min_time = int(conf.return_time_range()[0])
max_time = int(conf.return_time_range()[-1])

data_hor_bar = conf.read_data()
data_hor_bar = data_hor_bar.groupby([conf.return_area()]).size().reset_index(name = "count")
data_hor_bar = data_hor_bar.sort_values(by = ["count"], ascending = False)
data_hor_bar = data_hor_bar.head()

# def plot_hor_bar():
#     data_hor_bar[conf.return_area()] = data_hor_bar[conf.return_area()].astype("str") 
#     fig =  px.bar(data_hor_bar, x = "count", y = data_hor_bar[conf.return_area()], orientation='h')
#     return fig

#### read geojson data
with open(json_map, "r") as f:
    json_data = json.load(f)


geojson = dl.GeoJSON(
    data = json_data, 
    zoomToBoundsOnClick = False,
    id = "geojson")

#### define function to hover on map
def get_info(feature = None):
    header = [html.H4("Municipio")]
    if not feature:
        return header + ["Hoover over a state"]
    return header + [html.B(feature["properties"]["name"]), html.Br()]
#"{:} people / mi".format(feature["properties"]["codmunres"]), html.Sup("2")

cont = dbc.Card(
    [
        dbc.CardImg(src = "assets/form.svg", top = True, className = "card-img"),
        dbc.CardBody(
            [
                html.P(
                    "Casos: " + str(conf.return_data_size())
                )
            ],
            className = "card-text"
        ),
    ],
    className = "info-item",
)

date_range = dbc.Card(
    [
        dbc.CardImg(src = "assets/calendar.svg", top = True, className = "card-img"),
        dbc.CardBody(
            [
                html.P(
                    "De xxx a xxx"
                )
            ],
            className = "card-text"
        ),
    ],
    className = "info-item",
)


time_unit = dbc.Card(
    [
        dbc.CardImg(src = "assets/clock.svg", top = True, className = "card-img"),
        dbc.CardBody(
            [
                html.P(
                    "Unidade de tempo"
                )
            ],
            className = "card-text"
        ),
    ],
    className = "info-item",
)

layout = html.Div(

    id = "container",
    children = [

        ###Header
        html.Div(
            id = "header",
            children = [
                html.Div(
                    children = [
                        html.Img(
                            src = functions.encode_image(path_to_images + "brazil.png"), className = "header-img"),
                        html.H1(
                            conf.return_title(),
                            className = "header-title"
                        ),
                        html.A(html.Img(
                            src = functions.encode_image(path_to_images + "cidacs.jpg"), className = "header-logo"),
                            href = "https://cidacs.bahia.fiocruz.br/",
                            style ={"grid-column":"12 / span 1", "display":"grid"}
                        )
                    ],
                    className = "header-cotainer"
                )
            ],
            className = "header"
        ),

        ###information menu
        html.Div(
            id = "nav-bar",
            children = [
                html.Div(
                    children = [
                        cont,
                        date_range,
                        time_unit
                    ],
                    className = "info-bar-cotainer"
                )
                
            ],
            className = "info-bar"
        ),

        ###Main graphs
        html.Div(
            id = "main-body",
            children = [
                html.Div(
                    id = "wrapper",
                    children = [
                        html.Div(
                            id = "map",
                            children = [
                                dl.Map(
                                    center = [-16, -52],
                                    zoom = 4,
                                    children = [
                                        dl.TileLayer(),
                                        geojson,
                                        html.Div(
                                            children = get_info(), 
                                            id = "info", className = "info",
                                            style = {"position": "absolute", "top": "10px", "right": "10px", "z-index": "1000"})],
                                    style = {"border-radius":"8px"})
                                
                            ],
                            className ="leaflet-map"),
                        html.Div(
                            children = dt.DataTable(
                                id = "data_table",
                                columns=[{"id": " ", "name": " "},
                                         {"id": "count", "name": "Observado"},
                                         {"id": "expected", "name": "Esperado"},
                                         {"id": "rr", "name": "RR"}],
                                data = []
                                ),
                            className = "table"

                        ),
                        html.Div(
                            id = "time-series",
                            children = [
                                dcc.Graph(
                                    id = "time-series-graph",
                                    className = "ts-graph"
                                )
                            ],
                            className = "time-series"
                        )

                    ],
                    className = "wrapper"
                )
                


            ],
            className = "main-body"
        ),
        
        #Customize side-bar
        html.Div(
            id = "side-bar",
            children = [
                html.Div(
                    children = [
                        html.Br(),
                        html.Br(),
                        html.Label(
                            ["Selecione o intervalo de tempo",
                            dcc.RangeSlider(
                                id = "range-select",
                                min = min_time,
                                max = max_time,
                                step = 1,
                                marks = {
                                    min_time: {'label': str(min_time), 'style': {'color': '#77b0b1'}},
                                    max_time: {'label': str(max_time), 'style': {'color': '#77b0b1'}}

                                },
                                value = [min_time, max_time],
                                className = "side-bar-item")
                                ],
                                className = "side-bar-text"
                                ),
                        html.Br(),
                        html.Br(),
                        html.Label(
                            ["Selecione a unidade de tempo",
                            dcc.RadioItems(
                                id = "time-unit",
                                options = [
                                    {"label": "Dia", "value": "dia"},
                                    {"label": "Semana", "value": "semana"},
                                    {"label": "Mês", "value": "mes"},
                                    {"label": "Ano", "value": "ano"}
                                ],
                                value = "dia",
                                style={'margin-top': '10px'}
                            )],
                             className = "side-bar-text"
                             ),
                        html.Br(),
                        html.Br(),
                        html.Label(
                            ["Selecionar variável para Donut plot",
                            dcc.Dropdown(
                                id = "var_cat",
                                options = conf.return_cat(),
                                value = None,
                                multi = False
                            ),
                            ],
                            className = "side-bar-text"
                        ),
                        html.Br(),
                        html.Br(),
                        # html.Label(
                        #     ["Selecionar covariável 2",
                        #     dcc.Dropdown(
                        #         id = "var_num",
                        #         options = [
                        #             # {"label": conf.return_num()[0], "value": conf.return_num()[0]},
                        #             # {"label": conf.return_num()[1], "value": conf.return_num()[1]}
                        #         ],
                        #         value = None,
                        #         multi = False
                        #     ),
                        #     ],
                        #     className = "side-bar-text"
                        # )
                        
                    ],
                    className = "side-bar-container"
                )
            ],
            className = "side-bar"
        ),

        #customize side graph
        html.Div(
            id = "side-graph",
            children = [
                html.Div(
                    children = [
                        # dcc.Graph(
                        #     id = "hor_bar",
                        #     figure = plot_hor_bar(),
                        #     className = "side-graph-item"),
                        html.Label(
                            ["Donut Plot",
                                dcc.Graph(
                                    id = "donut_plot",
                                    className = "side-graph-item")],
                            className = "side-bar-text"
                                    

                        )
                    ],
                    className = "side-graph-container"

                )
            ],
            className = "side-graph"
        ),

        html.Div(
            id = "footer",
            children = [
                html.A(
                    children = html.I(className="fa fa-github"),
                    href ="#"
                ),
            ],
            className = "footer"
        )
    ],
    className = "container"
)



