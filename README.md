# Data Engineer Challenge

Projeto de Engenharia de Dados para um desafio técnico.

## Explicações

Este projeto utiliza o **Airflow** para orquestrar as tarefas em Python. A estrutura de dados segue a arquitetura de **Data Lakehouse**, adaptada com as pastas necessárias para organizar as camadas de dados.

### Estrutura Data Lakehouse

Estou utilizando a arquitetura **Medallion**, onde as camadas são organizadas da seguinte forma:

1. **data/raw_data**: Camada **Bronze** (dados brutos).
2. **data/staging_data**: Camada **Silver** (dados transformados).
3. **PostgreSQL**: Camada **Gold** (dados prontos para análise).

A camada Gold é armazenada diretamente no banco de dados, sem necessidade de salvá-la localmente visto que neste contexto não há necessidade de salvar em pastas.

## Como Executar

### Desenvolvimento

1. Instalar o **pre-commit**:
```bash
pip install pre-commit
pre-commit install
```

2. Instancie o ambiente:
```sh
make up
```

2. Pause o ambiente:
```sh
make stop
```

3. Mate o ambiente:
```sh
make down
```

4. Build o ambiente:
```sh
make build
```

## Ambiente de desenvolvimento

Acesse o ambiente de desenvolvimento
```sh
make sh
su airflow
```
