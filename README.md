# Market Basket Insights - Delivery 6

Este projeto é um desafio técnico com foco em duas frentes:

- aplicação da metodologia GitFlow no ciclo de desenvolvimento;
- análise de dados de um serviço de entrega para geração de inteligência de negócio.

## Objetivo

Realizar uma análise de cesta de compras (Market Basket Analysis) para identificar associações entre produtos. A proposta é utilizar métricas como suporte e frequência para encontrar itens com maior probabilidade de compra conjunta, apoiando estratégias de recomendação e vendas casadas.

## Tecnologias

- Python 3.12 (gerenciado via asdf)
- Pandas (manipulação e tratamento de dados)
- Streamlit (interface para dashboard)
- SQLite (persistência de dados processados)
- GitFlow (gestão de branches e fluxo de trabalho)

## Estrutura do projeto

```text
.
├── README.md
├── requirements.txt
├── data/
│   └── processed/
└── src/
```

## Como executar

1. Configure o Python 3.12 (preferencialmente com asdf).
2. Instale as dependências:

	```bash
	pip install -r requirements.txt
	```

3. Execute os scripts do projeto (carga, análise e dashboard) conforme forem adicionados na pasta `src/`.

## Status atual

O repositório já possui ambiente e dependências definidos. A implementação dos scripts de carga, análise e visualização está em andamento.