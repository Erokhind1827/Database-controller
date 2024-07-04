from PyQt5.QtWidgets import *
import psycopg2
import pandas as pd
import numpy as np
import os.path
import io 
import re
from datetime import datetime, timedelta
import random
import sys

class window(QWidget):
    def __init__(self):
        super().__init__()
        self.stacked_layout = QStackedLayout()
        self.db = ''
        self.user = ''
        self.password = ''
        self.port = 0
        self.final_query = ''

        connect_layout = QVBoxLayout()
        db_label = QLabel('Input database name:')
        user_label = QLabel('Input username:')
        password_label = QLabel('Input password:')
        port_label = QLabel('Input port:')
        self.db_text = QLineEdit()
        self.user_text = QLineEdit()
        self.user_text.setText('postgres')
        self.password_text = QLineEdit()
        self.password_text.setText('123456')
        self.port_text = QLineEdit()
        self.port_text.setText('5432')
        connect_button = QPushButton('Connect!')
        connect_button.clicked.connect(self.setConnection)

        connect_layout.addWidget(db_label)
        connect_layout.addWidget(self.db_text)
        connect_layout.addWidget(user_label)
        connect_layout.addWidget(self.user_text)
        connect_layout.addWidget(port_label)
        connect_layout.addWidget(self.port_text)
        connect_layout.addWidget(password_label)
        connect_layout.addWidget(self.password_text)
        connect_layout.addWidget(connect_button)

        self.connect_widget = QWidget(self)
        self.connect_widget.setLayout(connect_layout)
        self.stacked_layout.addWidget(self.connect_widget)
        self.setLayout(self.stacked_layout)

    def __del__(self):
        with io.open('final_query.txt', 'w') as file:
            file.write(self.final_query)

    def setConnection(self):
        self.db = self.db_text.text()
        self.user = self.user_text.text()
        self.password = self.password_text.text()
        self.port = int(self.port_text.text())

        try:
            self.conn = psycopg2.connect(dbname = self.db, user = self.user, host = "localhost", password = self.password, port = self.port)
            self.connection_succesful = True
            self.cur = self.conn.cursor()
            self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
            self.tables = self.cur.fetchall()

            count_layout = QVBoxLayout()
            self.set_count = QRadioButton()
            choice_label = QLabel('Choose your table:')
            self.choice_box = QComboBox()
            for table in self.tables:
                self.choice_box.addItem(table[0])
            result_label = QLabel('Amount of rows in chosen table is:')
            self.result = QLabel()
            self.result.setStyleSheet("font-size: 20px; color: red;")
            self.count_button = QPushButton('Count!')
            self.count_button.clicked.connect(self.count)
            

            count_layout.addWidget(self.set_count)
            count_layout.addWidget(choice_label)
            count_layout.addWidget(self.choice_box)
            count_layout.addWidget(result_label)
            count_layout.addWidget(self.result)
            count_layout.addWidget(self.count_button)
            
            generate_layout = QVBoxLayout()
            self.set_generate = QRadioButton()
            choose_table_label = QLabel('Choose the table to generate: ')
            self.table_box = QComboBox()
            self.table_box.addItem('Prestupniki')
            self.table_box.addItem('Sledovateli')
            self.table_box.addItem('Poterpevshye')
            self.table_box.addItem('Prestuplenia')
            self.table_box.addItem('Prestupniki_Prestuplenia')
            self.table_box.addItem('Poterpevshye_Prestuplenia')
            number_label = QLabel('Number of rows')
            self.number = QSpinBox()
            self.number.setRange(1, 10000)
            self.number.setValue(100)
            self.generate_button = QPushButton('Generate!')
            self.generate_button.clicked.connect(self.generate)
            
            generate_layout.addWidget(self.set_generate)
            generate_layout.addWidget(choose_table_label)
            generate_layout.addWidget(self.table_box)
            generate_layout.addWidget(number_label)
            generate_layout.addWidget(self.number)
            generate_layout.addWidget(self.generate_button)           
            self.set_generate.toggled.connect(self.stateChanged)
            self.set_generate.setChecked(True)

            main_layout = QHBoxLayout()
            main_layout.addLayout(generate_layout)
            main_layout.addLayout(count_layout)

            self.main_window = QWidget()
            self.main_window.setLayout(main_layout)
            self.stacked_layout.addWidget(self.main_window)
            self.stacked_layout.setCurrentWidget(self.main_window)

        except:
            msb = QMessageBox()
            msb.setText('Unable to connect :(')
            msb.exec()
            self.connection_succesful = False

    def updateState(self, state):
        self.stateChanged(1 - state)

    def stateChanged(self, state):
        self.choice_box.setEnabled(1 - state)
        self.count_button.setEnabled(1 - state)
        self.table_box.setEnabled(state)
        self.number.setEnabled(state)
        self.generate_button.setEnabled(state)

    def count(self):
        table_to_count = self.choice_box.currentText()
        self.cur.execute(f'SELECT count(*) from {table_to_count}')
        res = self.cur.fetchone()[0]
        self.result.setText(str(res))

    def generate(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        table_to_generate = self.table_box.currentText()
        number_of_rows = self.number.value()
        table_type = -1
        success = False



        #type_1
        if table_to_generate in ['Prestupniki', 'Sledovateli', 'Poterpevshye']:
            # self.cur.execute(f'drop table if exists {table_to_generate} cascade')
            table_type = 1
            df = pd.read_excel(open(r'names.xlsx', 'rb'))
            df = pd.DataFrame(df)
            columnss = ['id', 'surname', 'name', 'father_name', 'birth_date', 'sex', 'passport']
            types = ['real', 'text', 'text', 'text', 'date', 'varchar(1)', 'varchar(6)']
            if table_to_generate == 'Sledovateli':
                columnss += ['rank', 'division']
                types += ['text', 'int']
            if table_to_generate == 'Poterpevshye':
                columnss += ['phone_number', 'status']
                types += ['varchar(11)', 'text']
            data = []
            success = True
            for i in range(number_of_rows):
                sex = random.choice(['м', 'ж', 'м'])
                choose_from = df.loc[df['sex'] == sex]
                id = i + 1
                first_name = random.choice(choose_from['name'].tolist())
                last_name = random.choice(choose_from['surname'].tolist())
                middle_name = random.choice(choose_from['father_name'].tolist())
                birth_date = (datetime(1950, 1, 1) + \
                                timedelta(days=random.randint(0, 20000))).strftime('%d.%m.%Y')
                passport = str(random.randint(100000, 999999))
                data_to_append = [id, last_name, first_name, middle_name, birth_date, sex, passport]
                if table_to_generate == 'Sledovateli':
                    rank = random.choice(['Лейтенант', 'Полковник', 'Майор', 'Капитан', 'Подполковник'])
                    division = random.randint(1, 12)
                    data_to_append += [rank, division]
                if table_to_generate == 'Poterpevshye':
                    phone_number = '89' + str(random.randint(100000000, 999999999))
                    status = random.choice(['Свидетель', 'Потерпевший'])
                    data_to_append += [phone_number, status]
                data.append(data_to_append)



        #type_2
        elif table_to_generate == 'Prestuplenia':
            table_type = 2
            # self.cur.execute(f'drop table if exists Prestuplenia')
            try:
                self.cur.execute(f'select count(*) from Sledovateli')
                count = self.cur.fetchone()[0]
                if count > 0:
                    success = True
                    classification = pd.DataFrame(pd.read_excel(open(r'classification.xlsx', 'rb')))
                    columnss = ['id', 'type_pr', 'data', 'status', 'sledovatel', 'result']
                    types = ['real', 'int', 'date', 'bool', 'real', 'bool']
                    data = []
                    for i in range(number_of_rows):
                        id = i + 1
                        type_pr = random.choice(classification['type'].tolist())
                        start_year = 2019
                        date = (datetime(start_year, 1, 1) + \
                                    timedelta(days=random.randint(0, (2023 - start_year + 1) * 365))).strftime('%d.%m.%Y')
                        status = random.choice([True, True, True, True, True, False])
                        sledovatel, result = None, None
                        if status:
                            sledovatel = random.randint(1, count) 
                            result = random.choice([True, False, None])
                        data.append([id, type_pr, date, status, sledovatel, result])
                else:
                    msb = QMessageBox()
                    msb.setText('Table Sledovateli is empty!')
                    msb.exec()
            except:
                msb = QMessageBox()
                msb.setText('Table Sledovateli does not exist! Create?')
                msb.exec()
                self.conn.commit()
                self.cur.execute('Create table Sledovateli (id int);')
                self.conn.commit()
        


        # type_3
        else:
            table_type = 3
            columnss = re.split('_', table_to_generate)
            # columnss[1] = 'Prestuplenia'
            info = [0] * len(columnss)
            try:
                for l in range(len(columnss)):
                    self.cur.execute(f'select id from {columnss[l]}')
                    info[l] = [el[0] for el in self.cur.fetchall()]                       
                if len(info[0]) * len(info[1]) != 0:
                    success = True
                    data = []
                    types = ['real', 'real']
                    for i in range(number_of_rows):
                        while True:
                            data_to_append = [random.choice(el) for el in info]
                            if data_to_append not in data:
                                data.append(data_to_append)
                                break
                else:
                    msb = QMessageBox()
                    msb.setText(f'Table {columnss[0] if len(info[0]) == 0 else columnss[1]} is empty!')
                    msb.exec()
            except:
                msb = QMessageBox()
                msb.setText('One or both Tables do not exist! Create?')
                msb.exec()
                self.conn.commit()
                for l in range(len(columnss)):
                    self.cur.execute(f'Create table if not exists {columnss[l]} (id int);')
                    self.conn.commit()
                


        if success:
            new_df = pd.DataFrame(data, columns = columnss)
            file_name = f'{table_to_generate}.csv'
            new_df.to_csv(file_name, sep = ';', index=False, encoding='cp1251')
            
            # SQL:
            self.cur.execute(f'drop table if exists {table_to_generate} cascade')
            query = f'Create table {table_to_generate} (\n'
            for l in range(len(types)):
                query += f'{columnss[l]} {types[l]}'
                query += f' primary key' if (l == 0 and len(types) != 2) else f''
                query += f', \n' if l != len(types) - 1 else f''
            if len(types) == 2:
                query += f', \nprimary key ({columnss[0]}, {columnss[1]})'
            query += f'); \n'
            if table_type == 2:
                query += f'ALTER TABLE {table_to_generate} ADD CONSTRAINT Sledovatel_FK \nFOREIGN KEY (Sledovatel) \nREFERENCES Sledovateli (id) ON DELETE CASCADE; \n'
            query += f"copy {table_to_generate} from '{dir_path}/{table_to_generate}.csv' with(format csv, header, delimiter ';', encoding 'WIN1251'); \n\n"
            self.final_query += query
            self.cur.execute(query)
            os.remove(f'{dir_path}/{table_to_generate}.csv')
        
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
        self.tables = self.cur.fetchall()
        self.choice_box.clear()
        for table in self.tables:
            self.choice_box.addItem(table[0])
        self.conn.commit()

            

# change the current directory to the script directory
dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)
app = QApplication(sys.argv)
w = window()
w.show()
sys.exit(app.exec_())