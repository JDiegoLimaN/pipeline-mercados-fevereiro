from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit

def create_spark_session(app_name: str = "DataPipeline", master: str = "local[*]"):
    """
    Cria e retorna uma sessão Spark.

    Args:
        app_name (str): O nome da aplicação Spark.
        master (str): O endereço master Spark (ex: "local[*]" para local, "yarn" para cluster).

    Returns:
        SparkSession: A sessão Spark criada.
    """
    print(f"Iniciando a sessão Spark para a aplicação: {app_name}")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .getOrCreate()
    )
    print("Sessão Spark criada com sucesso!")
    return spark


def load_dataframe(spark: SparkSession, path: str, file_type: str, options: dict = None):
    """
    Carrega um DataFrame Spark de um determinado caminho, com base no tipo de arquivo.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        path (str): O caminho do arquivo a ser lido.
        file_type (str): O tipo do arquivo ('json' ou 'csv').
        options (dict, optional): Um dicionário de opções para o leitor Spark.
                                  Defaults para None.

    Returns:
        DataFrame: O DataFrame Spark carregado, ou None em caso de erro.
    """
    print(f"\nTentando ler o DataFrame de: {path}")
    df = None
    try:
        reader = spark.read

        # Aplica as opções de leitura, se existirem
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)

        # Lógica para ler JSON ou CSV
        if file_type.lower() == "json":
            df = reader.json(path)
        elif file_type.lower() == "csv":
            df = reader.csv(path)
        else:
            print(f"Erro: Tipo de arquivo '{file_type}' não suportado. Por favor, use 'json' ou 'csv'.")
            return None

        print(f"DataFrame lido com sucesso de: {path}!")
        print("\nSchema do DataFrame: ")
        df.printSchema()
        print("\nPrimeiras 5 linhas do DataFrame: ")
        df.show(5, truncate=False)
        print(f"Total de registros: {df.count()}.")
        return df

    except Exception as e:
        print(f"Erro ao ler DataFrame de {path}: {e}.")
        print("Por favor, verifique o caminho e as permissões da pasta.")
        return None
    

def process_dataframe(df: DataFrame, mapping: dict, required_columns: list):
    """
    Processa um DataFrame, renomeando colunas e adicionando colunas ausentes.

    Args:
        df (DataFrame): O DataFrame Spark a ser processado.
        mapping (dict): Um dicionário de mapeamento de colunas {nome_antigo: nome_novo}.
        required_columns (list): Uma lista de nomes de colunas esperadas após o processamento.

    Returns:
        DataFrame: O DataFrame Spark processado.
    """
    print("\n--- Iniciando o processamento do DataFrame ---")
    processed_df = df

    # Renomear colunas
    for old_name, new_name in mapping.items():
        if old_name in processed_df.columns:
            processed_df = processed_df.withColumnRenamed(old_name, new_name)
            print(f"Coluna '{old_name}' renomeada para '{new_name}'.")
        else:
            print(f"Aviso: Coluna original '{old_name}' não encontrada para renomeação. Pulando.")

    # Adicionar colunas ausentes com valores nulos (StringType por padrão)
    current_columns = [col.lower() for col in processed_df.columns] # Para comparação case-insensitive
    for col in required_columns:
        if col.lower() not in current_columns:
            processed_df = processed_df.withColumn(col, lit(None).cast(StringType()))
            print(f"Coluna '{col}' adicionada como nula.")
        else:
            print(f"Coluna '{col}' já existe no DataFrame.")

    print("\nSchema do DataFrame após processamento:")
    processed_df.printSchema()
    print("\nPrimeiras 5 linhas do DataFrame após processamento:")
    processed_df.show(5, truncate=False)
    print(f"Total de registros no DataFrame processado: {processed_df.count()}")
    print(f"Total de colunas no DataFrame processado: {len(processed_df.columns)}")
    print("--- Processamento do DataFrame concluído com sucesso! ---")
    return processed_df


def union_dataframes(df1: DataFrame, df2: DataFrame):
    """
    Realiza a união de dois DataFrames Spark por nome de coluna (unionByName).

    Args:
        df1 (DataFrame): O primeiro DataFrame Spark.
        df2 (DataFrame): O segundo DataFrame Spark.

    Returns:
        DataFrame: Um novo DataFrame resultante da união dos dois DataFrames,
                   ou None se um dos DataFrames de entrada for nulo.
    """
    if df1 is None or df2 is None:
        print("Erro: Não é possível unir DataFrames. Um ou ambos os DataFrames de entrada são nulos.")
        return None

    print("\n--- Iniciando a união dos DataFrames ---")
    try:
        # Garante que os schemas são compatíveis ou a união será mais complexa
        # No seu caso, a função process_dataframe já padroniza isso.
        
        # Realiza a união por nome da coluna.
        # Caso haja colunas com o mesmo nome, mas tipos diferentes, pode ocorrer erro.
        # Assumimos que o process_dataframe já cuidou da padronização dos tipos também.
        unified_df = df1.unionByName(df2, allowMissingColumns=True)

        print("DataFrames unidos com sucesso!")
        print("\nSchema do DataFrame unificado:")
        unified_df.printSchema()
        print("\nPrimeiras 10 linhas do DataFrame unificado:")
        unified_df.show(10, truncate=False) # Mostra um pouco mais para ver dados de ambas as fontes
        print(f"Total de registros no DataFrame unificado: {unified_df.count()}")
        print("--- União dos DataFrames concluída! ---")
        return unified_df

    except Exception as e:
        print(f"Erro ao unir DataFrames: {e}.")
        print("Verifique se as colunas e seus tipos são compatíveis entre os DataFrames.")
        return None
    

def save_dataframe(df: DataFrame, output_path: str, file_format: str, options: dict = None):
    """
    Salva um DataFrame Spark em um caminho e formato especificados.

    Args:
        df (DataFrame): O DataFrame Spark a ser salvo.
        output_path (str): O caminho onde o DataFrame será salvo.
        file_format (str): O formato do arquivo ('csv', 'parquet', 'json', etc.).
        options (dict, optional): Um dicionário de opções para o gravador Spark.
                                  Defaults para None.

    Returns:
        bool: True se o DataFrame foi salvo com sucesso, False caso contrário.
    """
    if df is None:
        print("Erro: Não é possível salvar um DataFrame nulo.")
        return False

    print(f"\nTentando salvar DataFrame em: {output_path} no formato {file_format}.")
    try:
        writer = df.write.mode("overwrite") # Modo padrão de sobrescrita

        if options:
            for key, value in options.items():
                writer = writer.option(key, value)

        if file_format.lower() == "csv":
            writer.csv(output_path)
        elif file_format.lower() == "parquet":
            writer.parquet(output_path)
        elif file_format.lower() == "json":
            writer.json(output_path)
        else:
            print(f"Erro: Formato de arquivo '{file_format}' não suportado para salvamento.")
            return False

        print(f"DataFrame salvo com sucesso em: {output_path}.")
        return True

    except Exception as e:
        print(f"Erro ao salvar DataFrame em {output_path}: {e}.")
        print("Por favor, verifique o caminho e as permissões de escrita.")
        return False