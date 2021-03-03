#!/usr/bin/env python3

from sys import argv
from os import path, system
from dbfread import DBF
import pandas as pd
import csv

folder = path.dirname(__file__)
blast_dbf = path.join(folder, 'blast_dbf')


class ReadDbf:
    """Converte o arquivo database inserido de acordo com o
    fluxograma  [+] dbc -> dbf -> csv [+]
    """

    def __init__(self, file_dbf, convert='convert'):
        """A instancia recebe um parametro o nome do arquivo dbf
        e abre uma lista para os itens que serão iterados no dbf
        """
        self.file_dbf = list(file_dbf)[0]

        if convert == 'convert':
            if path.isfile(self.file_dbf):
                self.read_dbf_to_csv(
                    self.__check_file_dbf())

        else:
            ...

    def __check_file_dbf(self):
        """Confere se o arquivo inserido está no formato dbf
        se estiver, o arquivo será convertido diretamente para csv,
        caso contrário o mesmo irá passar pela conversão para dbf
        através da ferramenta blast-dbf que se encontra no mesmo
        diretório deste script
        """
        if bool(self.file_dbf.endswith(('.dbf', '.DBF'))):
            return self.file_dbf

        elif bool(self.file_dbf.endswith(('.dbc', '.DBC'))):
            system(f"./blast-dbf {self.file_dbf} \
                   {self.file_dbf.split('.')[0]}.dbf")
            return self.file_dbf.split('.')[0] + '.dbf'

        else:
            print(f'O arquivo {self.file_dbf} não é válido')

    def read_dbf_to_csv(self, file_dbf):
        """Abre um arquivo com o nome do dbf splitado e substituindo a
        extensão .dbf por .csv. Depois é escrito o cabeçalho utilizando
        os campos tidos como fields_names pelo dbf, para posteriormente
        serem gravados como cabeçalho do arquivo csv.
        Aplica um loop para iterar os valores de dicionários dentro do
        arquivo dbf diretamente no arquivo csv.
        """

        dbf = DBF(file_dbf, encoding='iso-8859-1')

        with open('{}.csv'.format(file_dbf.split(".")[0]), 'w+') as csvfile:
            data = csv.writer(csvfile)
            data.writerow(dbf.field_names)
            for record in dbf:
                data.writerow(list(record.values()))


if __name__ == '__main__':
    ReadDbf(file_dbf=argv[1], convert=argv[2])
