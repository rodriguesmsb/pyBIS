#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Tue Jan 26 2021
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
from dash_extensions.javascript import Namespace, arrow_function
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from aux.functions import functions
import json
import numpy as np



### Indicates patch
path_to_data = "data/data.csv"
path_to_json = "conf/conf.json"
path_to_images = "assets/"



### Manipulate data
conf = functions(conf_file = path_to_json, data = path_to_data)
json_map = path_to_images + "maps/geojs-" + conf.set_json_map() + "-mun.json"

min_time = int(conf.return_time_range()[0])
max_time = int(conf.return_time_range()[-1])


data = conf.read_data()
data["date"] = conf.format_date(data[conf.return_time()])

#grouping initial data
ts = data.groupby([conf.return_area(), "date"]).size().reset_index(name = "count")
cases_per_city = data.groupby([conf.return_area()]).size().reset_index(name = "cases")



with open(json_map, "r") as f:
    json_data = json.load(f)

def search_on(id, data):
    return data.index(int(id))
    

for i in range(0,len(json_data["features"])):
    codmunres = json_data["features"][i]['properties']["id"][0:6]
    index_cases = search_on(id = codmunres, data = list(cases_per_city[conf.return_area()].values))
    cases = {"cases": cases_per_city["cases"][index_cases]}
    json_data["features"][i]['properties'].update(cases)



### Define functions that will be used on callbacks

#### define function to hover on map
def get_info(feature=None):
    header = [html.H4("Municipio")]
    if not feature:
        return header + ["Click on a state"]
    return header + [html.B(feature["properties"]["name"]), html.Br()]
#"{:} people / mi".format(feature["properties"]["codmunres"]), html.Sup("2")


def get_id(feature = None):
    if not feature:
        return ["Selecione um municipio"]
    return int(str(feature["properties"]["id"][0:6]))

def plotTs(df):
    cases_trace = go.Scatter(
        x  = df["date"],
        y =  df["count"],
        mode ='markers',
        name = "Fitted",
        line = {"color": "#d73027"}
    )
    data = [cases_trace]
    layout = go.Layout(yaxis = {"title": "Incidência"})
    return {"data": data, "layout": layout}


### Create a instance of Dash class
app = dash.Dash(__name__, 
external_stylesheets = ["https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css",
                        "https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500&display=swap"])
app.title = "Data visualization"


##options
quantiles = cases_per_city["cases"].quantile([0,.25,.5,.75, 0.9])

quantiles = [int(n) for n in quantiles.values]

classes = quantiles
colorscale = ['#FED976', '#FEB24C', '#FD8D3C', '#FC4E2A', '#800026']
style = dict(weight=2, opacity=1, color='white', dashArray='3', fillOpacity = 0.7)

# Create colorbar.
ctg = ["{}+".format(cls, classes[i + 1]) for i, cls in enumerate(classes[:-1])] + ["{}+".format(classes[-1])]
colorbar = dlx.categorical_colorbar(categories=ctg, colorscale=colorscale, width=300, height=30, position="bottomleft")

ns = Namespace("dlx", "choropleth")
### Define layouts
geojson = dl.GeoJSON(
    data = json_data,
    options=dict(style=ns("style")),
    zoomToBoundsOnClick = False,
    hoverStyle=arrow_function(dict(weight=2, color='#666', dashArray='', fillOpacity=0.2)),  # style applied on hover
    hideout=dict(colorscale=colorscale, classes=classes, style=style, colorProp = "cases"),
    id = "geojson")




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

app.layout = html.Div(

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
                                        colorbar,
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



@app.callback(Output("info", "children"), [Input("geojson", "click_feature")])
def info_hover(feature):
    return get_info(feature)


@app.callback(Output(component_id = "time-series-graph", component_property = "figure"),
              [Input(component_id = "geojson", component_property = "click_feature"),
               Input(component_id = "time-unit", component_property = "value")])
def update_Graph(feature, time_unit):
    filtered_df = ts[ts[conf.return_area()] == get_id(feature)]
    if time_unit == "semana":
        filtered_df["date"] = pd.to_datetime(
            filtered_df["date"].dt.week.astype(str) +
            filtered_df["date"].dt.year.astype(str).add("-2"),
            format = "%W%Y-%w"
        )
        filtered_df = filtered_df.groupby([conf.return_area(), "date"])["count"].sum().reset_index(name = "count")
    elif time_unit == "mes":
        filtered_df["date"] = pd.to_datetime(
            "01" +
            filtered_df["date"].dt.month.astype(str) +
            filtered_df["date"].dt.year.astype(str),
            format = "%d%m%Y"
        )
        filtered_df = filtered_df.groupby([conf.return_area(), "date"])["count"].sum().reset_index(name = "count")
    elif time_unit == "ano":
        filtered_df["date"] = pd.to_datetime(
            "01" +
            "01" +
            filtered_df["date"].dt.year.astype(str),
            format = "%d%m%Y"
        )
        filtered_df = filtered_df.groupby([conf.return_area(), "date"])["count"].sum().reset_index(name = "count")
    return plotTs(df = filtered_df)


@app.callback([Output(component_id = "data_table", component_property = "data")],
             [Input(component_id = "geojson", component_property = "click_feature")])
def update_table(feature):
    filtered_df = ts[ts[conf.return_area()] == get_id(feature)]
    summary = filtered_df["count"].describe().reset_index()[1:]
    summary["count"] = np.round(summary["count"], 2)

    summary = summary.rename(columns = {"index": " "})
    return [summary.to_dict("records")]


@app.callback(Output(component_id = "donut_plot", component_property = "figure"),
              [Input(component_id = "geojson", component_property = "click_feature"),
               Input(component_id = "var_cat", component_property = "value")])
def update_donut(feature, selected_var):
    filtered_df = data[data[conf.return_area()] == get_id(feature)]
    filtered_df = filtered_df.groupby([selected_var]).size().reset_index(name = "count")
    filtered_df["prop"] = np.round(filtered_df["count"] / np.sum(filtered_df["count"]),2)
    donut = px.pie(data_frame = filtered_df, names = filtered_df[selected_var], values = filtered_df["prop"], hole = .4)
    return donut




if __name__ == '__main__':
    app.run_server()