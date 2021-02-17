#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Tue Jan 26 2021
@author: Moreno rodrigues rodriguesmsb@gmail.com
"""

import dash_core_components as dcc
import dash_table as dt
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_leaflet as dl
import pandas as pd
from aux.functions import functions




path_to_data = "data/data.csv"
path_to_json =  "conf/conf.json"

conf = functions(conf_file = path_to_json, data = path_to_data)


path_to_map = "assets/maps/geojs-" + conf.set_json_map() + "-mun.json"

cont = dbc.Card(
    [
        dbc.CardImg(src = "assets/form.svg", top = True, className = "card-img"),
        dbc.CardBody(
            [
                html.P(
                    "Contagem"
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
                        html.Img(src = functions.encode_image("assets/brazil.png"), className = "header-img"),
                        html.H1(
                            "Análise espaço temporal",
                            className = "header-title"
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
                                        dl.GeoJSON(url = path_to_map)],
                                    style = {"border-radius":"8px"})
                                
                            ],
                            className ="leaflet-map"),
                        html.Div(
                            id = "table",
                            children = [dt.DataTable(
                                id = "table"
                            )

                            ],
                            className = "table"

                        ),
                        html.Div(
                            id = "time-series",
                            children = [
                                dcc.Graph(
                                    id = "time-series-cases",
                                    config = {"displayModeBar": False},
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
                            ["Selecione uma variável", 
                             dcc.Dropdown(id = "var-select", className = "side-bar-item")],
                            className = "side-bar-text"
                             ),
                        html.Br(),
                        html.Br(),
                        html.Label(
                            ["Selecione o intervalo de tempo",
                            dcc.RangeSlider(
                                id = "range-select",
                                min = 2010,
                                max = 2019,
                                step = 1,
                                marks = {
                                    2010: {'label': '2010', 'style': {'color': '#77b0b1'}},
                                    2015: {'label': '2015', 'style': {'color': '#77b0b1'}},
                                    2019: {'label': '2019', 'style': {'color': '#77b0b1'}}

                                },
                                value = [2010, 2019],
                                className = "side-bar-item")
                                ],
                                className = "side-bar-text"
                                )
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
                        dcc.Graph(id = "cov1", className = "side-graph-item"),
                        dcc.Graph(id = "cov2", className = "side-graph-item")
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




