#!/usr/bin/env python3

import os, platform

def confirm_dir_exist(diretorio):
    data = [x for x in os.listdir(diretorio)]

def get_system():
    if platform.system().lower() == 'linux':
        try:
            os.mkdir(os.path.expanduser('~/Documentos/files_csv/'))
        except:
            pass
        diretorio = os.path.expanduser('~/Documentos/files_csv/')

    elif platform.system().lower() == 'windows':
        try:
            os.mkdir(os.path.expanduser('~\\Meus Documentos\\files_csv\\'))
        except:
            pass
        diretorio = os.path.expanduser('~\\Meus Documentos\\files_csv\\')

    confirm_dir_exist(diretorio)
