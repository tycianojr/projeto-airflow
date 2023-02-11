# Projeto 3 - DAG Airflow

# Este pipeline carrega os dados da fonte (arquivo JSON), aplica as transformações, salva em formato CSV e insere no DW.

# Imports
import json
import pendulum # Permite gerenciar datas
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import conecta_db.conecta_db as conn_dw

# Caminho da pasta com a fonte de dados (altere o caminho da pasta na máquina onde executar o pipeline)
input = '/home/tyciano/projeto-airflow/projeto/entrada/dados_hoteis.json'

# Função para carregar arquivo JSON (fonte de dados)
def carrega_arquivo():
    with open(input, 'r') as f:
        data = json.load(f)
    return data

# Função para extração de atributos dos hotéis
def func_extrai_atributos(path: str):

    # Carrega o arquivo
    dados = carrega_arquivo()

    # Lista para os dados extraídos
    lista_dados = []

    # Loop com bloco try/except
    try:
        for linha in dados:

            # Extrai as colunas desejadas e insere cada linha na lista de dados
            lista_dados.append({'ID': linha['id'],
                                'Name': linha['name'],
                                'Type': linha['type'],
                                'rating': linha['rating'],
                                'Awards': len(linha['awards']),
                                'RankingPosition': linha['rankingPosition'],
                                'HotelClass': linha['hotelClass'],
                                'NumberOfReviews': linha['numberOfReviews'],
                                'priceRange': linha['priceRange']
            })
    except:
        print("Ocorreu algum problema ao carregar a lista de dados.")
        pass

    # Cria um dataframe do pandas com os dados extraídos do arquivo
    df1 = pd.DataFrame(lista_dados)

    # Grava o dataframe em arquivo CSV
    df1.to_csv(path, index = False)

# Função para extrair o range de preço dos hotéis e a posição do hotel no ranking
def func_prices_range_position(path: str):

    # Carrega o arquivo
    dados = carrega_arquivo()

    # Lista para os dados
    lista_dados = []

    # Loop com bloco try/except
    try:
        for linha in dados:
            lista_dados.append({'priceRange': linha['priceRange'],
                                'RankingPosition': linha['rankingPosition']
            })
    except:
        print("Ocorreu algum problema ao carregar a lista de dados.")
        pass

    # Dataframe
    df2 = pd.DataFrame(lista_dados)
    df2.to_csv(path, index = False)

# Função para extrair nomes, prêmios e avaliações dos hotéis
def func_hotel_award_rating(path: str):

    # Carrega o arquivo
    dados = carrega_arquivo()

    # Lista
    lista_dados = []

    # Loop
    try:
        for linha in dados:
            lista_dados.append({'Name': linha['name'],
                                'Awards': len(linha['awards']),
                                'rating': linha['rating']
            })
    except:
        print("Ocorreu algum problema ao carregar a lista de dados.")
        pass

    # Dataframe
    df3 = pd.DataFrame(lista_dados)
    df3.to_csv(path, index = False)

# Função para extrair número de avaliações, classe do hotel e avaliação dos usuários
def func_reviews_class_rating(path: str):

    # Carrega o arquivo
    dados = carrega_arquivo()

    # Lista
    lista_dados = []

    # Loop
    try:
        for linha in dados:
            lista_dados.append({'NumberOfReviews': linha['numberOfReviews'],
                                'HotelClass': linha['hotelClass'],
                                'rating': linha['rating']
            })
    except:
        print("Ocorreu algum problema ao carregar a lista de dados.")
        pass

    # Dataframe
    df4 = pd.DataFrame(lista_dados)
    df4.to_csv(path, index = False)

# Função para inserir os dados em cada tabela no DW
def func_insert_dimensions():

    # Carrega o arquivo
    dados_dim_1 = pd.read_csv('/home/tyciano/projeto-airflow/projeto/stage/resultado2.csv', index_col = False, delimiter = ',')
    dados_dim_1.head()
    
    # Desativa o modo de segurança do MySQL
    sql = "SET SQL_SAFE_UPDATES = 0;"
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Desativa a checagem de chaves estrangeiras
    sql = "SET FOREIGN_KEY_CHECKS = 0;"
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Limpa a tabela antes da carga
    sql = "TRUNCATE TABLE tb_prices_rang"
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Loop pelos dados para inserir na tabela no banco de dados
    for i, row in dados_dim_1.iterrows():
        sql = "INSERT INTO tb_prices_rang(pricerange, rankingposition) VALUES (%s,%s)"
        conn_dw.cursor.execute(sql, tuple(row))
        conn_dw.conn.commit()

    # Carrega o arquivo
    dados_dim_2 = pd.read_csv('/home/tyciano/projeto-airflow/projeto/stage/resultado3.csv', index_col = False, delimiter = ',')
    dados_dim_2.head()

    # Limpa a tabela antes da carga
    sql = "TRUNCATE TABLE tb_hotels"
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Loop pelos dados para inserir na tabela no banco de dados
    for i, row in dados_dim_2.iterrows():
        sql = "INSERT INTO tb_hotels(name, awards, rating) VALUES (%s,%s,%s)"
        conn_dw.cursor.execute(sql, tuple(row))
        conn_dw.conn.commit()

    # Carrega o arquivo
    dados_dim_3 = pd.read_csv('/home/tyciano/projeto-airflow/projeto/stage/resultado4.csv', index_col = False, delimiter = ',')
    dados_dim_3.head()

    # Limpa a tabela antes da carga
    sql = "TRUNCATE TABLE tb_hotel_reviews"
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Loop pelos dados para inserir na tabela no banco de dados
    for i, row in dados_dim_3.iterrows():
        sql = "INSERT INTO tb_hotel_reviews(numberofreviews, hotelclass, rating) VALUES (%s,%s,%s)"
        conn_dw.cursor.execute(sql, tuple(row))
        conn_dw.conn.commit()

# Função para inserir na tabela FATO
def func_insert_fact():

    # Limpa a tabela antes da carga
    sql = "TRUNCATE TABLE tb_fact"
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Query para insert na tabela fato
    sql = "INSERT INTO tb_fact(pricesrange_id, hotels_id, reviews_id, number_of_hotels, number_ofrange_prices, max_hotelclass, min_hotelclass, " \
          "max_ofreviews, min_ofreviews) select tb_prices_rang.id, tb_hotels.id, tb_hotel_reviews.id, count(name), count(pricerange), max(hotelclass), min(hotelclass), " \
          "max(numberofreviews), min(numberofreviews) FROM tb_prices_rang, tb_hotels, tb_hotel_reviews GROUP BY tb_prices_rang.id, tb_hotels.id, tb_hotel_reviews.id; "

    # Executa a query
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()
    
    # Ativa a checagem de chaves estrangeiras
    sql = "SET FOREIGN_KEY_CHECKS = 1;"
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    sql = "SET SQL_SAFE_UPDATES = 1;"
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()


# DAG para executação no Airflow
with DAG(dag_id = 'DAG_ProjetoDW', start_date = pendulum.datetime(2023, 1, 1, tz = "UTC"), schedule_interval = '@Daily', catchup = False) as dag:

    # Tarefa de carga de dados
    task1_carrega_dados = PythonOperator(
        task_id = 'carrega_dados',
        python_callable = carrega_arquivo,
        dag = dag
    )

    # Tarefa de extração de atributos
    task2_extrai_dados = PythonOperator(
        task_id = 'transforma_dados',
        python_callable = func_extrai_atributos,
        op_kwargs = {'path': '/home/tyciano/projeto-airflow/projeto/stage/resultado1.csv'},
        dag = dag
    )

    # Tarefa de extração do range de preço
    task3_prices_range_position = PythonOperator(
        task_id = 'prices_range_position',
        python_callable = func_prices_range_position,
        op_kwargs = {'path': '/home/tyciano/projeto-airflow/projeto/stage/resultado2.csv'},
        dag = dag
    )

    # Tarefa de extração de dados dos hotéis
    task4_hotel_award_rating = PythonOperator(
        task_id = 'hotel_award_rating',
        python_callable = func_hotel_award_rating,
        op_kwargs = {'path': '/home/tyciano/projeto-airflow/projeto/stage/resultado3.csv'},
        dag=dag
    )

    # Tarefa de extração de avaliações
    task5_reviews_class_rating = PythonOperator(
        task_id = 'reviews_class_rating',
        python_callable = func_reviews_class_rating,
        op_kwargs = {'path': '/home/tyciano/projeto-airflow/projeto/stage/resultado4.csv'},
        dag = dag
    )

    # Tarefa de inserção de dados nas dimensões
    task6_insert_dimensions = PythonOperator(
        task_id = 'insert_dimensions',
        python_callable = func_insert_dimensions,
        dag = dag
    )

    # Tarefa de inserção de dados na tabela FATO
    task7_insert_fact = PythonOperator(
        task_id = 'insert_fact',
        python_callable = func_insert_fact,
        dag = dag
    )

# Ordem das tarefas (isso de fato é o grafo direcionado)
task1_carrega_dados >> task2_extrai_dados >> task3_prices_range_position >> task4_hotel_award_rating >> task5_reviews_class_rating >> task6_insert_dimensions >> task7_insert_fact



