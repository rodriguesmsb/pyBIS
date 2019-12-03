import glob as gb
import re
from tqdm import tqdm



def set_data(dataset):
    dataset = dataset.upper()
    if dataset == "SINASC":
        path = "/home/rodriguesms/Documents/Data_lake/Raw/Bancos_SINASC/CSV_format/"
        pattern = "DN*[A-Z][A-Z]"
    elif dataset == "SIM":
        path = "/home/rodriguesms/Documents/Data_lake/Raw/Bancos_SIM/CSV_format/"
        pattern = "DO*[A-Z][A-Z]"
    elif dataset == "AIH":
        path = "/home/rodriguesms/Documents/Data_lake/Raw/Bancos_AIH/CSV_format/"
        pattern = "RD*[A-Z][A-Z]"
    return([path, pattern])


#Define function to point files
def select_files(dataset, state = False, years = False, extension = ".csv"):
    """
        This function is used to create a list of datasets based on one of
        the brazilian information system e.g. AIH, SINASC, SINAN, SIM
    """
    path = dataset[0]
    pattern = dataset[1]
    results = {}
    if re.search(r"_AIH", path):
        for year in years:
            if pattern:
                files = gb.glob(path + pattern + str(year)[2:4] + "*" + extension)
            results.update({year:files})
        return(results)
    else:
        try:
            for year in years:
                if pattern:
                    files = gb.glob(path + pattern + "*" + str(year) + "*" + extension)
                else:
                    files = gb.glob(path + "*" + extension)
                results.update({year:files})
            return(results)
        except:
            if pattern:
                files = gb.glob(path + pattern + "*" + extension)
            else:
                files = gb.glob(path + "*" + extension)
            return(files)