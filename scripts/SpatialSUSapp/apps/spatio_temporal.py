#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Tue Jan 26 2021
@author: Moreno rodrigues rodriguesmsb@gmail.com
"""

import dash_core_components as dcc
import dash_html_components as html
import dash_leaflet as dl


import pandas as pd


layout = html.Div(
    id = "container",
    children = [
        html.Div(
            id = "header",
            children = ["header"],
            className = "header"
        ),
        html.Div(
            id = "nav-bar",
            children = ["Navigation"],
            className = "nav-bar"
        ),
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
                                    center = [-20, -54],
                                    zoom = 3.2,
                                    children = [dl.TileLayer()],
                                    style = {"border-radius":"8px"})
                                
                            ],
                            className ="leaflet-map"),
                        html.Div(
                            id = "table",
                            children = ["Summary statistics"],
                            className = "table"

                        ),
                        html.Div(
                            id = "time-series",
                            children = ["Time-series"],
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
                )
            ],
            className = "footer"
        )
    ],
    className = "container"
)


