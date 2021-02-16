
import pandas as pd
import json



class functions:
    def __init__(self, conf_file, data):
        self.conf_file = conf_file
        self.data = data
        
    
    def set_pathname(self):
        print(self.conf_file)
        return  self.conf_file["type"]