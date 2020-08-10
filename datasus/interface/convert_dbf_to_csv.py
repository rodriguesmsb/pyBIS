#!/usr/bin/env python3

from sys import argv
from os import path, listdir, system
from dbfread import DBF
import pandas as pd


class ReadDbf:
    """Converte o arquivo database inserido de acordo com o
    fluxograma  [+]!!! dbc -> dbf -> csv !!![+]
    """

    def __init__(self, file_dbf):
        """A instancia recebe um parametro o nome do arquivo dbf
        e abre uma lista para os itens que serão iterados pelo dbf
        """
        self.file_dbf = file_dbf
        self.list_items = []

        if path.isfile(self.file_dbf):
            self.read_dbf(
                self.check_file_dbf(file_dbf))

    def check_file_dbf(self, file_dbf):
        """Confere se o arquivo inserido está no formato dbf
        se estiver, o arquivo será convertido diretamente para csv,
        caso contrário o mesmo irá passar pela conversão para dbf
        através da ferramenta blast-dbf que se encontra no mesmo
        diretório deste script
        """
        if bool(file_dbf.endswith(('.dbf', '.DBF'))):
            return file_dbf

        elif bool(file_dbf.endswith(('.dbc', '.DBC'))):
                system(f"blast-dbf {file_dbf} {file_dbf.split('.')[0]}.dbf")
                return file_dbf.split('.')[0] + '.dbf'

        else:
            print(f'O arquivo {file_dbf} não é válido')

    def read_dbf(self, file_dbf):
        """Lê as chaves e valores contidos no arquivo dbf para
        depois transforma-lo em um dataframe e gravar diretamente
        como um csv formatado com cabeçalho na primeira linha e
        separador igual a ','
        """
        malign_dbf = DBF(file_dbf, encoding='ISO-8859-1', load=True)

        for linha in range(0, len(malign_dbf)):
            self.list_items.append(
                pd.DataFrame(
                    dict(list(malign_dbf.records[linha].items())),
                    index=[ind for ind in range(len(malign_dbf.records[linha])
                                                )]))

        pd.concat(self.list_items).to_csv(file_dbf.split('.')[0] +
                                          '.csv', index=False)
