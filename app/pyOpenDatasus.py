import os
import json
import csv
import requests
from requests.auth import HTTPBasicAuth
from PyQt5.QtCore import QObject, pyqtSignal


class PyOpenDatasus(QObject):

    sig = pyqtSignal(int)

    def __init__(self, api, state, date_min=None, date_max=None):
        super().__init__()

        self.api = api
        self.state = state.lower()
        self.date_min = date_min
        self.date_max = date_max

    def __read_json(self):
        local = os.path.dirname(__file__)
        api = os.path.join(local, '../conf/api.json')
        with open(api, 'r') as filejson:
            df = json.load(filejson)['apis'][self.api]
            return df

    def __check_if_date_isNone(self):
        if self.date_min is None or self.date_max is None:
            return True
        else:
            return False

    def __create_form(self):
        auth = None
        body = None

        if self.api == 'notificacao_sg':
            auth = HTTPBasicAuth(
                'user-public-notificacoes', 'Za4qNXdyQNSa9YaA'
            )
            if self.__check_if_date_isNone():
                    body = {
                        'query': {
                            'match_all': {}
                        }
                    }
            else:
                body = {
                    'size': 10000,
                    'query': {
                        'range': {
                            'dataNotificacao': {
                                'gte': self.date_min,
                                'lte': self.date_max
                            }
                        }
                    }
                }


        elif self.api == 'vacinacao':
            auth = HTTPBasicAuth(
                'imunizacao_public',
                'qlto5t&7r_@+#Tlstigi'
            )
            if self.__check_if_date_isNone():
                    body = {
                        'query': {
                            'match_all': {}
                        }
                    }
            else:
                body = {
                    'size': 10000,
                    'query': {
                        'bool': {
                            'must': [{
                                'match': {
                                    'estabelecimento_uf': self.state
                                }
                            },
                                {
                                    'range': {
                                        'vacina_dataAplicacao': {
                                            'gte': self.date_min,
                                            'lte': self.date_max
                                        }
                                    }
                                }
                                     ]
                        }
                    }
                }

        elif self.api == 'leitos_covid19':
            auth = HTTPBasicAuth(
                'user-api-leitos', 'aQbLL3ZStaTr38tj'
            )
            if self.__check_if_date_isNone():
                    body = {
                        'query': {
                            'match_all': {}
                        }
                    }
            else:
                body = {
                    'size': 10000,
                    'query': {
                        'bool': {
                            'must': [{
                                'match': {
                                    'estadoSigla': self.state
                                }
                            },
                                {
                                    'range': {
                                        'dataNotificacaoOcupacao': {
                                            'gte': self.date_min,
                                            'lte': self.date_max
                                        }
                                    }
                                }
                                     ]
                        }
                    }
                }

        return auth, body

    def download(self, filename):

        url, idx = self.__read_json()['url'], self.__read_json()['idx']
        auth, body = self.__create_form()

        if self.api == 'notificacao_sg':
            r = requests.post(
                url +
                idx.replace('*', self.state) +
                '?scroll=3m',
                auth=auth,
                json=body
            )

        else:
            r = requests.post(
                url +
                idx +
                '?scroll=3m',
                auth=auth,
                json=body
            )

        sid = r.json()['_scroll_id']
        scroll_size = r.json()['hits']['total']['value']
        total = scroll_size

        data = r.json()['hits']['hits']

        header = list(data[0]['_source'].keys())

        count = len(data)
        ratio = round((float(total / count) * 100 - 6), 1)

        with open(filename, 'w') as f:
            filecsv = csv.DictWriter(f, fieldnames=header,
                                     extrasaction='ignore')
            filecsv.writeheader()

            while scroll_size > 0:

                count += len(data)
                ratio = round((float(count / total) * 100 - 6), 1)
                percent = int(round(100 * ratio / (100 - 6), 1))
                self.sig.emit(percent)

                for hits in data:
                    filecsv.writerow(hits['_source'])

                body = {
                    'scroll': '3m',
                    'scroll_id': sid,
                }

                r = requests.post(url + '_search/scroll',
                                  auth=auth, json=body)

                try:
                    sid = r.json()['_scroll_id']
                    scroll_size = len(r.json()['hits']['hits'])
                    data = r.json()['hits']['hits']
                except KeyError:
                    break


if __name__ == '__main__':
    test = PyOpenDatasus('vacinacao', 'ro', '2021-09-06', 'now')
    test.download('vacinacao.csv')
