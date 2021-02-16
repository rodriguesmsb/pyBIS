
import pandas as pd





class functions:
    def __init__(self, conf_file, data):
        self.conf_file = conf_file
        self.data = data
        
    def set_pathname(self):
        return self.conf_file["type"]