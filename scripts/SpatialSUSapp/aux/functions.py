
import pandas as pd
import base64
import json


class functions:
    def __init__(self, conf_file, data):
        with open(conf_file, 'r') as f:
            self.conf_file = json.load(f)
        self.data = pd.read_csv(data)
     
    def set_pathname(self):
        return self.conf_file["type"]

    def encode_image(image_file):
        ''' 
        Function to encode a image in a format that allows its plot on html.Fig
        '''
        encode = base64.b64encode(open(image_file, "rb").read())
        return "data:image/jpeg;base64,{}".format(encode.decode())

    def set_json_map(self):
        return self.conf_file["area"]

    def ibg6(json_map):
        for i in range(len(json_map["features"])):
            data = {"codmunres": int(str(json_map["features"][i]["properties"]["id"])[0:6])}
            json_map["features"][i]['properties'].update(data)
            return json_map

    def format_date(self, series):
        if self.conf_file["sistema"] == "SINASC" or self.conf_file["sistema"] == "SIM":
            date = pd.to_datetime(series, format = "%d%m%Y", errors = "coerce")
        else:
            date = pd.to_datetime(series, format = "%Y-%m-%d", errors = "coerce")
        return date

    def return_title(self):
        return self.conf_file["name"]
        
    def read_data(self):
        # self.data = pd.read_csv(self.data)
        return self.data

    def return_time(self):
        return self.conf_file["time_col"]
    
    def return_time_range(self):
        return self.conf_file["time_range"]

    def return_data_size(self):
        return self.data.shape[0]

    def return_area(self):
        return self.conf_file["id_area"]

    def return_cat(self):
        results = []
        for var, var_type in zip(self.conf_file["var_type"], self.conf_file["var_col"]):
            if var_type == "Categorica":
                results.append({"label": var, "value": var})
        return results
    






    


    

    
