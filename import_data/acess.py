import pyodbc
import pandas as pd
import numpy as np
# import multiprocessing

def get_connection():
    server = 'mssql_server,1433'
    # server = 'localhost,1433' 
    database = 'master' 
    username = 'sa' 
    password = 'Your_password123' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='
        +database+';UID='+username+';PWD='+ password)
    return cnxn

def create_table(cnxn):
    cursor = cnxn.cursor()
    cursor.execute('''
        CREATE TABLE PARTICIPANTE (
                NU_INSCRICAO BIGINT PRIMARY KEY,
                NU_ANO INTEGER,
                TP_FAIXA_ETARIA INTEGER,
                TP_SEXO VARCHAR(1),
                TP_ESTADO_CIVIL INTEGER,
                TP_COR_RACA INTEGER,
                TP_NACIONALIDADE INTEGER,
                TP_ST_CONCLUSAO INTEGER,
                TP_ANO_CONCLUIU INTEGER,
                TP_ESCOLA INTEGER,
                TP_ENSINO INTEGER,
                IN_TREINEIRO INTEGER,

                CO_MUNICIPIO_ESC INTEGER,
                NO_MUNICIPIO_ESC VARCHAR(150),
                CO_UF_ESC INTEGER,
                SG_UF_ESC VARCHAR(2),
                TP_DEPENDENCIA_ADM_ESC INTEGER,
                TP_LOCALIZACAO_ESC INTEGER,
                TP_SIT_FUNC_ESC INTEGER,

                CO_MUNICIPIO_PROVA INTEGER,
                NO_MUNICIPIO_PROVA VARCHAR(150),
                CO_UF_PROVA INTEGER,
                SG_UF_PROVA VARCHAR(2),

                TP_PRESENCA_CN INTEGER,
                TP_PRESENCA_CH INTEGER,
                TP_PRESENCA_LC INTEGER,
                TP_PRESENCA_MT INTEGER,
                CO_PROVA_CN INTEGER,
                CO_PROVA_CH INTEGER,
                CO_PROVA_LC INTEGER,
                CO_PROVA_MT INTEGER,
                NU_NOTA_CN INTEGER,
                NU_NOTA_CH INTEGER,
                NU_NOTA_LC INTEGER,
                NU_NOTA_MT INTEGER,
                TX_RESPOSTAS_CN VARCHAR(45),
                TX_RESPOSTAS_CH VARCHAR(45),
                TX_RESPOSTAS_LC VARCHAR(45),
                TX_RESPOSTAS_MT VARCHAR(45),
                TP_LINGUA INTEGER,
                TX_GABARITO_CN VARCHAR(45),
                TX_GABARITO_CH VARCHAR(45),
                TX_GABARITO_LC VARCHAR(50),
                TX_GABARITO_MT VARCHAR(45),

                TP_STATUS_REDACAO INTEGER,
                NU_NOTA_COMP1 FLOAT,
                NU_NOTA_COMP2 FLOAT,
                NU_NOTA_COMP3 FLOAT,
                NU_NOTA_COMP4 FLOAT,
                NU_NOTA_COMP5 FLOAT,
                NU_NOTA_REDACAO FLOAT,

                Q001 VARCHAR(1),
                Q002 VARCHAR(1),
                Q003 VARCHAR(1),
                Q004 VARCHAR(1),
                Q005 INTEGER,
                Q006 VARCHAR(1),
                Q007 VARCHAR(1),
                Q008 VARCHAR(1),
                Q009 VARCHAR(1),
                Q010 VARCHAR(1),
                Q011 VARCHAR(1),
                Q012 VARCHAR(1),
                Q013 VARCHAR(1),
                Q014 VARCHAR(1),
                Q015 VARCHAR(1),
                Q016 VARCHAR(1),
                Q017 VARCHAR(1),
                Q018 VARCHAR(1),
                Q019 VARCHAR(1),
                Q020 VARCHAR(1),
                Q021 VARCHAR(1),
                Q022 VARCHAR(1),
                Q023 VARCHAR(1),
                Q024 VARCHAR(1),
                Q025 VARCHAR(1)
        );
    ''')

    cnxn.commit()

def cleaning(data):
    #NUMERIC VALIDATION
    lista = ['NU_INSCRICAO', 'NU_ANO', 'TP_FAIXA_ETARIA', 'Q005', 'TP_ESTADO_CIVIL',
             'TP_COR_RACA', 'TP_NACIONALIDADE', 'TP_ST_CONCLUSAO', 'TP_ANO_CONCLUIU',
             'TP_ESCOLA', 'TP_ENSINO', 'IN_TREINEIRO', 'CO_MUNICIPIO_ESC',
             'CO_UF_ESC', 'TP_DEPENDENCIA_ADM_ESC', 'TP_LOCALIZACAO_ESC', 'TP_SIT_FUNC_ESC',
             'CO_MUNICIPIO_PROVA', 'CO_UF_PROVA', 'TP_PRESENCA_CN', 'TP_PRESENCA_CH',
             'TP_PRESENCA_LC', 'TP_PRESENCA_MT', 'CO_PROVA_CN', 'CO_PROVA_CH', 'CO_PROVA_LC',
             'CO_PROVA_MT', 'NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'TP_LINGUA',
             'TP_STATUS_REDACAO', 'NU_NOTA_COMP1', 'NU_NOTA_COMP2', 'NU_NOTA_COMP3',
             'NU_NOTA_COMP4', 'NU_NOTA_COMP5', 'NU_NOTA_REDACAO']

    for col in lista:
        if str(data[col].dtype) == 'object' :
            data[col] = data[col].str.replace(r"[^\d]+",'')
            data[col] = data[col].replace('', np.nan).astype(float)

    data = data[ data['NU_INSCRICAO'].notna() ]    

    #ALPHANUMERIC VALIDATION
    lista = ['TX_RESPOSTAS_CN', 'TX_RESPOSTAS_CH', 'TX_RESPOSTAS_LC', 'TX_RESPOSTAS_MT',
             'TX_GABARITO_CN', 'TX_GABARITO_CH', 'TX_GABARITO_MT']
    for col in lista:
        data[col] = data[col].str.replace(r"[^a-eA-E]+",'.').str.slice(0, 45).str.upper()

    data['TX_GABARITO_LC'] = data['TX_GABARITO_LC'].str.replace(r"[^a-eA-E]+",'.').str.slice(0, 50).str.upper()

    lista = ['Q'+'0'*(3-len(str(i)))+str(i) for i in range(1, 26)]
    lista.pop(4)
    for col in lista:
        data[col] = data[col].str.replace(r"[^a-zA-Z]+",'').str.slice(0, 1).str.upper()

    data['NO_MUNICIPIO_PROVA'] = data['NO_MUNICIPIO_PROVA'].str.replace("'", "").str.slice(0, 150)
    data['NO_MUNICIPIO_ESC'] = data['NO_MUNICIPIO_ESC'].str.replace("'", "").str.slice(0, 150)

    data['SG_UF_PROVA'] = data['SG_UF_PROVA'].str.slice(0, 2)
    data['SG_UF_ESC'] = data['SG_UF_ESC'].str.slice(0, 2)
    data['TP_SEXO'] = data['TP_SEXO'].str.replace(r"[^a-zA-Z]+",'').str.slice(0, 1).str.upper()

    return data

def insert(data, cnxn):
    cursor = cnxn.cursor()

    data_columns = str(list(data.columns))
    data_columns = data_columns.replace('[', '(').replace(']', ')').replace("'", "")
    data_values = str(list(data.itertuples(index=False, name=None)))
    data_values = data_values.replace('[', '').replace(']', '')
    data_values = data_values.replace(" nan,", " NULL,").replace(" na,", " NULL,")
    data_values = data_values.replace(" nan)", " NULL)").replace(" na)", " NULL)")

    cursor.execute("INSERT INTO PARTICIPANTE "+data_columns+" VALUES "+data_values+";")
    cnxn.commit()

def load_data():
    cnxn = get_connection()
    create_table(cnxn)
    CHUNCK_SIZE = 1000
    reader = pd.read_csv('MICRODADOS_ENEM_2020.csv', sep=';', encoding='unicode_escape', chunksize = CHUNCK_SIZE)
    for data in reader:
        data = cleaning(data)
        insert(data, cnxn)
    cnxn.close()

def print_top():
    cnxn = get_connection()
    cursor = cnxn.cursor()
    cursor.execute('SELECT TOP 10 NU_INSCRICAO FROM PARTICIPANTE;')
    for row in cursor:
        print(row)
    cnxn.close()

load_data()
print_top()

