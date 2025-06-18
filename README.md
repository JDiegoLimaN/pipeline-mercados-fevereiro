# Pipeline de Fusão de Dados de Mercados (Fevereiro)

## Visão Geral

Este projeto implementa um pipeline de dados utilizando PySpark para carregar, processar, unificar e persistir dados de vendas de duas fontes diferentes (Empresa A - JSON e Empresa B - CSV) referentes ao mês de fevereiro. O objetivo é consolidar os dados em um formato padronizado para análises futuras.

## Estrutura do Projeto

-   `fusao_mercados_fev.py`: Contém as funções modulares do pipeline (criação da sessão Spark, carregamento de dados, processamento, união e salvamento).
-   `main_pipeline.py` (ou nome do seu script principal): Orquestra a execução das funções do pipeline.
-   `data_raw/`: Pasta para os dados brutos de entrada (ex: `empresaA.json`, `empresaB.csv`).
-   `data_processed/`: Pasta para os dados processados e unificados de saída.
-   `README.md`: Este arquivo.

## Pré-requisitos

Para executar este pipeline, você precisará ter:

-   **Apache Spark** (versão X.X.X, se souber, pode especificar)
-   **Python 3.x**
-   **Pyspark** (instale via `pip install pyspark`)

## Como Executar

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/SeuUsuario/pipeline-mercados-fevereiro.git](https://github.com/SeuUsuario/pipeline-mercados-fevereiro.git)
    cd pipeline-mercados-fevereiro
    ```
2.  **Organize os dados:** Coloque seus arquivos `empresaA.json` e `empresaB.csv` na pasta `data_raw/`.
3.  **Execute o pipeline:**
    ```bash
    spark-submit main_pipeline.py
    ```
    (Ou `python main_pipeline.py` se você configurou o ambiente PySpark de outra forma)

## Saída

O pipeline gerará um arquivo CSV consolidado (`dados_combinados.csv`) na pasta `data_processed/`, contendo os dados de ambas as empresas padronizados e unidos.

## Próximos Passos (Opcional)

-   Adicionar validação e ajuste de tipos de dados mais robustos.
-   Implementar testes unitários para as funções do pipeline.
-   Explorar o salvamento em formato Parquet para melhor performance.
