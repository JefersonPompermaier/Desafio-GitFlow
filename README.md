# Market Basket Insights - Delivery 6

Este projeto é um desafio técnico com foco em duas frentes:

- aplicação da metodologia GitFlow no ciclo de desenvolvimento;
- análise de dados de um serviço de entrega para geração de inteligência de negócio.

## Objetivo

Realizar uma análise de cesta de compras (Market Basket Analysis) para identificar associações entre produtos. O fluxo do projeto carrega os dados brutos, trata inconsistências e gera tabelas prontas para validação e análise.

## Linguagem

- Python 3.12 (gerenciado via asdf)

## Bibliotecas Principais

- Pandas (manipulação e tratamento de dados)
- SQLite (persistência de dados processados)
- GitFlow (gestão de branches e fluxo de trabalho)

## Scripts do projeto

- [src/ingestao.py](src/ingestao.py): carrega os CSVs de `data/raw/` para o banco SQLite.
- [src/tratamento.py](src/tratamento.py): executa a limpeza, validação e padronização dos dados.

## Arquivos de apoio

- [src/sql/](src/sql): consultas SQL usadas pelo tratamento.
- `data/processed/delivery_database.db`: banco SQLite gerado pelo fluxo.


## Estrutura do projeto

```text
.
├── README.md
├── requirements.txt
├── data/
│   ├── raw/
│   └── processed/
└── src/
    ├── ingestao.py
    ├── tratamento.py
    └── sql/
```

## Como executar

1. Configure o Python 3.12 (preferencialmente com asdf).
2. Instale as dependências:


	```bash
	pip install -r requirements.txt
	```

3. Execute os scripts do projeto na ordem desejada:

	```bash
	python src/ingestao.py
	python src/tratamento.py
	```

## Observações

- Os arquivos `.db` e o ambiente virtual já estão ignorados por padrão em [.gitignore](.gitignore).
