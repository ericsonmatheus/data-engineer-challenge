# Data Engineer Challenge

Este repositório apresenta a solução para um desafio técnico na área de Engenharia de Dados, focado em boas práticas para extração, transformação e armazenamento de dados.

## Explicações

Este projeto utiliza o **Apache Airflow** para orquestrar as tarefas desenvolvidos em Python. A estrutura de dados segue a arquitetura **Data Lakehouse**, tendo a separação das camadas (bronze, silver, gold), garantindo um fluxo eficiente e bem estruturado dos dados.

## Pré-requisitos

- Docker e Docker Compose
- Python 3.10 ou superior (para desenvolvimento local)

### Estrutura Data Lakehouse

Estou utilizando a arquitetura **Medallion**, onde as camadas são organizadas da seguinte forma:

1. **data/raw_data**: Camada **Bronze** (dados brutos).
2. **data/staging_data**: Camada **Silver** (dados transformados).
3. **PostgreSQL**: Camada **Gold** (dados prontos para análise).

A camada Gold é armazenada diretamente no banco de dados, sem a necessidade de salvá-la localmente, já que, neste contexto, não há necessidade de mantê-la em pastas.

## Configuração

### Instalação e Execução

1. Clone este repositório:
```bash
git clone https://github.com/ericsonmatheus/data-engineer-challenge.git
cd data-engineer-challenge
```

2. Instalar o **pre-commit**:
```bash
pip install pre-commit
pre-commit install
```

3. Inicie os serviços com Makefile:
```sh
make up
```

4. Acesse a interface web do Airflow:
```
http://localhost:8080
```
- Usuário padrão: `airflow`
- Senha padrão: `senhaairflow`

5. Ative a DAG `sales_data_pipeline_dag` na interface do Airflow.

### Makefile

1. Instancie o ambiente:
```sh
make up
```

2. Pause o ambiente:
```sh
make stop
```

3. Remove o ambiente:
```sh
make down
```

4. Build o ambiente:
```sh
make build
```

5. Acesse o ambiente de desenvolvimento:
```sh
make sh
su airflow
```

## Fluxo da DAG

A DAG segue o seguinte fluxo de execução:

1. **Extração de Dados** (executadas em paralelo caso existam workers)
   - `extract_sales_data`: Extrai dados de vendas do PostgreSQL
   - `extract_employees_data`: Extrai dados de funcionários da API
   - `extract_categories_data`: Extrai dados de categorias do arquivo Parquet

Obs.: Todas as tarefas salvam os dados extraídos no formato Parquet dentro da pasta `raw_data` no sistema de arquivos local do container.

## Estrutura de Diretórios de Saída

Os dados extraídos são salvos na seguinte estrutura:

```
data/
└── raw_data/
    ├── sales/
    │   └── YYYY-MM-DD/
    │       └── sales_data.parquet
    ├── employees/
    │   └── YYYY-MM-DD/
    │       └── employees_data.parquet
    └── categories/
        └── YYYY-MM-DD/
            └── categories_data.parquet
```
