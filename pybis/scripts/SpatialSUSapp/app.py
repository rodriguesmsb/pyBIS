#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Tue Jan 26 2021
@author: Moreno rodrigues rodriguesmsb@gmail.com
"""

import dash

app = dash.Dash(__name__, suppress_callback_exceptions = True)
server = app.server
