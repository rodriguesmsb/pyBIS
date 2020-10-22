"""Interface gráfica para aplicação pydatasus.py"""


from sys import argv, exit
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QPushButton,
                             QProgressBar, QComboBox, QGroupBox, QGridLayout,
                             QLabel, QSpinBox, QTableWidget, QTableWidgetItem)
from PyQt5.QtCore import Qt, pyqtSlot, pyqtSignal


class Download(QWidget):

    def __init__(self):
        super().__init__()
        self.setWindowTitle('Iniciar')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)
        self.estados = {
            'Acre': ['Rio Branco', 'AC', 'Norte'],
            'Alagoas': ['Maceió', 'AL', 'Nordeste'],
            'Amapá': ['Macapá', 'AP', 'Norte'],
            'Amazonas': ['Manaus', 'AM', 'Norte'],
            'Bahia': ['Salvador', 'BA', 'Nordeste'],
            'Ceará': ['Fortaleza', 'CE', 'Nordeste'],
            'Distrito Federal': ['Brasília', 'DF',
                                 'Centro-Oeste'],
            'Espírito Santo': ['Vitória', 'ES', 'Sudeste'],
            'Goiás': ['Goiânia', 'GO', 'Centro-Oeste'],
            'Maranhão': ['São Luís', 'MA', 'Nordeste'],
            'Mato Grosso': ['Cuiabá', 'MT', 'Centro-Oeste'],
            'Mato Grosso do Sul': ['Campo Grande', 'MS',
                                   'Centro-Oeste'],
            'Minas Gerais': ['Belo Horizonte', 'MG', 'Sudeste'],
            'Pará': ['Belém', 'PA', 'Norte'],
            'Paraíba': ['João Pessoa', 'PB', 'Nordeste'],
            'Paraná': ['Curitiba', 'PR', 'Sul'],
            'Pernambuco': ['Recife', 'PE', 'Nordeste'],
            'Piauí': ['Teresina', 'PI', 'Nordeste'],
            'Rio de Janeiro': ['Rio de Janeiro', 'RJ',
                               'Sudeste'],
            'Rio Grande do Norte': ['Natal', 'RN', 'Nordeste'],
            'Rio Grande do Sul': ['Porto Alegre', 'RS', 'Sul'],
            'Rondônia': ['Porto Velho', 'RO', 'Norte'],
            'Roraima': ['Boa Vista', 'RR', 'Norte'],
            'Santa Catarina': ['Florianópolis', 'SC', 'Sul'],
            'São Paulo': ['São Paulo', 'SP', 'Sudeste'],
            'Sergipe': ['Aracaju', 'SE', 'Nordeste'],
            'Tocantins': ['Palmas', 'TO', 'Norte']
        }

        self.group_system = QGroupBox('Sistemas')
        self.grid_sys = QGridLayout()
        self.grid_sys.setSpacing(50)

        self.sistema = QComboBox()
        self.sistema.addItems(['SIH', 'SIM', 'SINAN', 'SINASC'])
        self.bases = QComboBox()
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.locais = QComboBox()
        self.locais.addItems(['TODOS', 'ESTADO', 'REGIÃO'])
        self.locais.currentTextChanged.connect(self.estado_ou_regiao)
        self.estados_regioes = QComboBox()

        self.ano_inicial_label = QLabel('ANO INICIAL:')
        self.ano_final_label = QLabel('ANO FINAL:')
        self.ano_inicial = QSpinBox()
        self.ano_inicial.setRange(2010, 2019)
        self.ano_final = QSpinBox()
        self.ano_final.setRange(2010, 2019)
        self.spin_cores_label = QLabel('SETAR CORES:')
        self.spin_cores = QSpinBox()
        self.spin_memoria_label = QLabel('SETAR MEMORIA:')
        self.spin_memoria = QSpinBox()
        self.carregar_banco = QPushButton('CARREGAR BANCO')
        self.visualizar_banco = QPushButton('VISUALIZAR BANCO')

        self.lista_botoes = [
            self.sistema, self.bases, self.locais, self.estados_regioes,
            self.ano_inicial, self.ano_final, self.spin_cores,
            self.spin_memoria, self.carregar_banco, self.visualizar_banco
        ]

        self.grid_sys.addWidget(self.sistema, 0, 0)
        self.grid_sys.addWidget(self.bases, 1, 0)
        self.grid_sys.addWidget(self.progress_bar, 2, 0, 2, 2)
        self.grid_sys.addWidget(self.locais, 0, 1)
        self.grid_sys.addWidget(self.estados_regioes, 1, 1)

        self.group_system.setLayout(self.grid_sys)

        self.group_funct = QGroupBox('Opções')
        self.grid_funct = QGridLayout()

        self.grid_funct.addWidget(self.ano_inicial_label, 0, 0)
        self.grid_funct.addWidget(self.ano_final_label, 1, 0)
        self.grid_funct.addWidget(self.ano_inicial, 0, 1, Qt.AlignLeft)
        self.grid_funct.addWidget(self.ano_final, 1, 1, Qt.AlignLeft)

        self.grid_funct.addWidget(self.spin_cores_label, 0, 2)
        self.grid_funct.addWidget(self.spin_cores, 0, 3, Qt.AlignLeft)
        self.grid_funct.addWidget(self.spin_memoria_label, 1, 2)
        self.grid_funct.addWidget(self.spin_memoria, 1, 3, Qt.AlignLeft)

        self.group_funct.setLayout(self.grid_funct)

        self.group_botoes = QGroupBox()
        self.grid_buttons = QGridLayout()

        self.grid_buttons.addWidget(self.carregar_banco, 0, 0)
        self.grid_buttons.addWidget(self.visualizar_banco, 0, 1)

        self.group_botoes.setLayout(self.grid_buttons)

        self.tabela = QTableWidget(10, 150)

        self.group_table = QGroupBox('Tabela')
        self.grid_table = QGridLayout()
        self.grid_table.addWidget(self.tabela)
        self.group_table.setLayout(self.grid_table)

        self.main_layout = QGridLayout()
        self.main_layout.setSpacing(10)

        self.main_layout.addWidget(self.group_system, 0, 0)
        self.main_layout.addWidget(self.group_funct, 0, 1)
        self.main_layout.addWidget(self.group_botoes, 1, 1)
        self.main_layout.addWidget(self.group_table, 2, 0, 1, 0)

        self.setLayout(self.main_layout)

        self.show()

    @pyqtSlot(str)
    def estado_ou_regiao(self, text):
        self.estados_regioes.clear()

        if text == 'ESTADO':
            self.estados_regioes.addItems(list(self.estados.keys()))

            return list(self.estados.keys())

        elif text == 'REGIÃO':
            regioes = ['Norte', 'Nordeste', 'Centro-Oeste', 'Sul', 'Sudeste']
            self.estados_regioes.addItems(regioes)

            return regioes

        elif text == 'TODOS':
            self.estados_regioes.addItem('TODOS OS ESTADOS FORAM SELECIONADOS')

            return list(self.estados.keys())


if __name__ == '__main__':
    app = QApplication(argv)
    download = Download()
    exit(app.exec_())
