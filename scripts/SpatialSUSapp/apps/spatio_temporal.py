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



conf = functions(conf_file = path_to_json, data = "data/data.csv")


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
    className = "nav-item",
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
    className = "nav-item",
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
    className = "nav-item",
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

        ###Navigation menu
        html.Div(
            id = "nav-bar",
            children = [
                html.Div(
                    children = [
                        cont,
                        date_range,
                        time_unit
                    ],
                    className = "nav-bar-div"
                )
                
            ],
            className = "nav-bar"
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
                                    zoom = 3.45,
                                    children = [
                                        dl.TileLayer(),
                                        dl.GeoJSON(url = "/assets/maps/brazil.json")],
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

        html.Div(
            id = "side-bar",
            children = ["Side bar"],
            className = "side-bar"
        ),
        html.Div(
            id = "sid-graph",
            children = ["Side Graph"],
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




