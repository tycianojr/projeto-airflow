# Pipeline de dados com Apache Airflow

## Introdução:

O objetivo deste projeto foi desenvolver uma orquestração de pipeline de dados com o Apache Airflow, para enriquecer um Datawarehouse a partir de um ETL desenvolvido em Python. Foi extraído dados do site *tripadvisor* com informações para alimentar as seguintes estruturas:

- Dimensão com range de preço e posição no ranking;
- Dimensão com prémios e avaliação dos usuários;
- Dimensão com número de avalições, classe do hotel e avaliação do usuário;
- Fato com as chaves estrangeira e os fatos.


## Descrição do processamento:

### Módulo de Conexão ao MySQL:
``` 
# Import
import mysql.connector as msql

# Cria conexão
print("\nConectando ao SGBD MySQL...")
conn = msql.connect(host = 'localhost', user = 'svc-thsaj', password = '*******', database = "dw-mysql")

# Abre um cursor
print("\nConexão feita com sucesso...")
cursor = conn.cursor() 
```


### Criação das tabelas:
```
# Dimensão com range de preço (pricerange) e posição no ranking (rankingposition)
cursor.execute("CREATE TABLE IF NOT EXISTS tb_prices_rang(id INT PRIMARY KEY NOT NULL AUTO_INCREMENT, pricerange VARCHAR (30), "
               "rankingposition INT)")

# Dimensão com prêmios (awards) e avaliação de usuário (rating)
cursor.execute("CREATE TABLE IF NOT EXISTS tb_hotels(id INT PRIMARY KEY NOT NULL AUTO_INCREMENT, name VARCHAR (100), awards INT, "
               "rating DOUBLE )")

# Dimensão com número de avaliações (numberofreviews), classe do hotel (hotelclass) e avaliação de usuário (rating)
cursor.execute("CREATE TABLE IF NOT EXISTS tb_hotel_reviews(id INT PRIMARY KEY NOT NULL AUTO_INCREMENT, numberofreviews INT, "
               "hotelclass DOUBLE , "
               "rating DOUBLE )")

# Cria a tabela FATO com as chaves estrangeiras e os fatos
print("\nCriando tabela fato...")
cursor.execute("CREATE TABLE IF NOT EXISTS tb_fact(pricesrange_id INT, hotels_id INT, reviews_id INT, "
               "number_of_hotels INT, number_ofrange_prices INT, max_hotelclass INT, min_hotelclass INT, "
               "max_ofreviews INT, min_ofreviews INT, KEY pricesrange_id (pricesrange_id), KEY hotels_id (hotels_id), "
               "KEY reviews_id (reviews_id), CONSTRAINT fact_ibfk_1 FOREIGN KEY (pricesrange_id) REFERENCES "
               "tb_prices_rang (id), CONSTRAINT fact_ibfk_2 FOREIGN KEY (hotels_id) REFERENCES tb_hotels (id), "
               "CONSTRAINT fact_ibfk_3 FOREIGN KEY (reviews_id) REFERENCES tb_hotel_reviews (id))")

print("\nEstrutura do DW criada com sucesso!\n")
```
### ETL:

Detalhes da criação dos métodos para ETL no link abaixo:

[ETL - Python](https://github.com/tycianojr/projeto-airflow/blob/main/dags/pipeline.py)

### Dag para execução no Airflow:

```
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
```

### Interface Airflow:

Graph:

![Graph]("C:\Users\devty\Documents\Projetos\projeto-airflow\img\graph.pn"g)

Grid:

![Grid]("C:\Users\devty\Documents\Projetos\projeto-airflow\img\grid.png")

Gantt:

![Grid]("C:\Users\devty\Documents\Projetos\projeto-airflow\img\gantt.png")










