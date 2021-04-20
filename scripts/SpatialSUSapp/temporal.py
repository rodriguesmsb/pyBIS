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
from statsmodels.tsa.seasonal import seasonal_decompose



### Indicates path
try:
    path_to_data = "scripts/SpatialSUSapp/data/data.csv"
    path_to_json = "scripts/SpatialSUSapp/conf/conf.json"
    path_to_images = "scripts/SpatialSUSapp/assets/"
    conf = functions(conf_file = path_to_json, data = path_to_data)
except:
    path_to_data = "data/data.csv"
    path_to_json = "conf/conf.json"
    path_to_images = "assets/"
    conf = functions(conf_file = path_to_json, data = path_to_data)


### Reading data
data = conf.read_data()
data["date"] = conf.format_date(data[conf.return_time()])

###Get time values
min_time = int(conf.return_time_range()[0])
max_time = int(conf.return_time_range()[-1])


### Get ts for each city
ts = data.groupby([conf.return_area(), "date"]).size().reset_index(name = "count")
ts["week"] = pd.to_datetime(ts["date"].dt.week.astype(str) +
                            ts["date"].dt.year.astype(str).add("-2"),
                            format = "%W%Y-%w")

ts["month"] = pd.to_datetime("01" + 
                             ts["date"].dt.month.astype(str) +
                             ts["date"].dt.year.astype(str),
                             format = "%d%m%Y")
ts["weekday"] = ts["date"].dt.day_name()
ts["month_name"] = ts["month"].apply(lambda x: x.strftime("%b"))


weekly_series = ts.groupby([conf.return_area(), "week"])["count"].sum().reset_index(name = "count")
weekly_series = weekly_series.rename(columns = {"week": "date"})

monthly_series = ts.groupby([conf.return_area(),"month"])["count"].sum().reset_index(name = "count")
monthly_series = monthly_series.rename(columns = {"month": "date"})

daily_heat_map = ts.groupby([conf.return_area(), "weekday", "week"])["count"].sum().reset_index(name = "count")
daily_heat_map["week"] = daily_heat_map["week"].dt.week

map_week_day = {"Monday": "Segunda", "Tuesday": "Terça", "Wednesday": "Quarta", 
                "Thursday": "Quinta", "Friday": "Sexta", "Saturday": "Sábado",
                "Sunday": "Domingo"}

daily_heat_map["weekday"] = daily_heat_map["weekday"].map(map_week_day)


daily_heat_map["weekday"] = pd.Categorical(values = daily_heat_map["weekday"],
                                           categories = ["Segunda", "Terça", "Quarta", 
                                                         "Quinta", "Sexta", "Sábado", "Domingo"],
                                           ordered = True)

#Decomposing time series
#stl return 

test = monthly_series.groupby(["date"])["count"].sum().reset_index(name = "count")



def decomp(df, model = "multiplicative", time = 12):
    df.index = df["date"]
    df = df["count"]
    result = seasonal_decompose(df, model = model, period = time)
    return {"observed": result.observed, "seasonal": result.seasonal, 
            "trend": result.trend, "resid": result.resid}
    

cities_code = set(ts[conf.return_area()])


def plotTs(df, Title):
    cases_trace = go.Scatter(
        x  = df["date"],
        y =  df["count"],
        mode ='lines',
        name = "Fitted",
        line = {"color": "rgb(255, 101, 131)"},
        marker = {"color": "rgb(255, 101, 131)"},
    )
    data = [cases_trace]
    layout = go.Layout(
        title = Title,
        yaxis = {"title": "Incidência"},
        xaxis = {'showgrid': False},
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = "white")
    return {"data": data,"layout": layout}


def plotHeatmap(df, x, y, z, Title):
    my_hovertemplate = (
        "<i>Semana</i>: %{y}<br>" +
        "<i>Dia da Semana</i>: %{x}<br>" +
        "<i>Incidencia</i>: %{z}" +
        "<extra></extra>") # Remove trace info
    trace = go.Heatmap(
        x = df[x],
        y = df[y],
        z = df[z],
        hovertemplate = my_hovertemplate,
        colorscale = "Picnic",
        hoverongaps = False,
        showscale = False,
        xgap = 1,
        ygap = 1)
    data = [trace]
    layout = go.Layout(
        title = Title,
        yaxis = {"title": "Incidência"},
        xaxis = {'showgrid': False},
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = "white")
    return {"data": data,"layout": layout}



def return_city(data):
        results = []
        results.append({"label": "Todas", "value": "all"})
        for city in cities_code:
            results.append({"label": city, "value": city})
        return results



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
                        start_date = date(min_time,1,1),
                        end_date_placeholder_text = 'MM/DD/YYYY',
                        clearable = True,
                        with_portal = True,
                )],
                className = "date-picker"),

                html.Label(
                    ["Filtrar por município",
                    dcc.Dropdown(
                        id = "city_picker",
                        searchable = True,
                        options = return_city(cities_code),
                        value = "all",
                        placeholder = "Selecione um municipio"
                        )],
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
                            [dcc.Graph(id = "monthly_series")],
                            className = "monthly-series"
                        ),
                        html.Div(
                            [dcc.Graph(id = "daily_heat_map")],
                            className = "heat-series"
                        ),
                        html.Div(
                            [dcc.Graph(id = "series_decomp")],
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


@app.callback(Output(component_id = "daily_series", component_property = "figure"),
              [Input(component_id = "city_picker", component_property = "value")])
def update_Graph(city):
    if city == "all":
        new_ts = ts.groupby(["date"])["count"].sum().reset_index(name = "count")
    else:
        new_ts = ts[ts[conf.return_area()] == int(city)]
        new_ts = new_ts.groupby(["date"])["count"].sum().reset_index(name = "count")
    return plotTs(new_ts, Title = "Incidência diária")


@app.callback(Output(component_id = "weekly_series", component_property = "figure"),
              [Input(component_id = "city_picker", component_property = "value")])
def update_Graph(city):
    if city == "all":
        new_ts = weekly_series.groupby(["date"])["count"].sum().reset_index(name = "count")
    else:
        new_ts = weekly_series[weekly_series[conf.return_area()] == int(city)]
        new_ts = new_ts.groupby(["date"])["count"].sum().reset_index(name = "count")
    return plotTs(new_ts, Title = "Incidência semanal")


@app.callback(Output(component_id = "monthly_series", component_property = "figure"),
              [Input(component_id = "city_picker", component_property = "value")])
def update_Graph(city):
    if city == "all":
        new_ts = monthly_series.groupby(["date"])["count"].sum().reset_index(name = "count")
    else:
        new_ts = monthly_series[monthly_series[conf.return_area()] == int(city)]
        new_ts = new_ts.groupby(["date"])["count"].sum().reset_index(name = "count")
    return plotTs(new_ts, Title = "Incidência mensal")


@app.callback(Output(component_id = "daily_heat_map", component_property = "figure"),
              [Input(component_id = "city_picker", component_property = "value")])
def update_Graph(city):
    if city == "all":
        new_ts = daily_heat_map.groupby(["weekday", "week"])["count"].mean().reset_index(name = "count")
    else:
        new_ts = daily_heat_map[daily_heat_map[conf.return_area()] == int(city)]
        new_ts = new_ts.groupby(["weekday", "week"])["count"].mean().reset_index(name = "count")
    return plotHeatmap(new_ts, x = "weekday", y = "week", z = "count", Title = "Incidência Média de Notificações Diária")



#Change this for trend plot based on stl
@app.callback(Output(component_id = "series_decomp", component_property = "figure"),
              [Input(component_id = "city_picker", component_property = "value")])
def update_Graph(city, time = 365, compartiment = "trend"):

    if city == "all":
        new_ts  = ts.groupby(["date"])["count"].sum().reset_index(name = "count")
        new_ts = new_ts[["date", "count"]]
        ts_decomp = decomp(new_ts, model = "multiplicative", time = time)
        ts_decomp = pd.DataFrame.from_dict(ts_decomp)
        ts_decomp["date"] = ts_decomp.index

    else:
        new_ts = ts[ts[conf.return_area()] == int(city)]
        new_ts  = new_ts.groupby(["date"])["count"].sum().reset_index(name = "count")
        new_ts = new_ts[["date", "count"]]
        ts_decomp = decomp(new_ts, model = "multiplicative", time = time)
        ts_decomp = pd.DataFrame.from_dict(ts_decomp)
        ts_decomp["date"] = ts_decomp.index

    if compartiment == "trend":
        ts_decomp["count"] = ts_decomp["trend"]
        return(plotTs(ts_decomp, Title = "Decomposição STL"))

    
        
       

            
if __name__ == '__main__':
    app.run_server(debug=True)
