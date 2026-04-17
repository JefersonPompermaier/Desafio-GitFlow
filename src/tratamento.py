import sqlite3
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).parent.parent
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
DB_PATH = PROCESSED_DIR / "delivery_database.db"


def limpeza_caracteres(texto):
    if not isinstance(texto, str):
        return texto
    mapa = {
        "Ã§Ã£": "çã",
        "Ã´": "ô",
        "Ã¡": "á",
        "Ã©": "é",
        "Ã": "á",
    }
    for erro, correcao in mapa.items():
        texto = texto.replace(erro, correcao)
    return texto


def _parse_datas(series):
    datas = pd.to_datetime(series, format="%m/%d/%Y", errors="coerce")
    faltantes = datas.isna()
    if faltantes.any():
        datas.loc[faltantes] = pd.to_datetime(series[faltantes], errors="coerce")
    return datas


def processar_silver():
    print("Tratamento iniciado")

    if not DB_PATH.exists():
        raise FileNotFoundError(f"Banco nao encontrado em: {DB_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        print("Limpando tabela de produtos...")
        df_prod = pd.read_sql("SELECT * FROM produtos", conn)
        if "nome_produto" in df_prod.columns:
            df_prod["nome_produto"] = df_prod["nome_produto"].apply(limpeza_caracteres)
        if "categoria" in df_prod.columns:
            df_prod["categoria"] = df_prod["categoria"].apply(limpeza_caracteres)
        df_prod.to_sql("produtos_silver", conn, if_exists="replace", index=False)

        print("Padronizando datas de pedidos...")
        df_ped = pd.read_sql("SELECT * FROM pedidos", conn)
        if "data_pedido" in df_ped.columns:
            df_ped["data_pedido"] = _parse_datas(df_ped["data_pedido"])
        df_ped.to_sql("pedidos_silver", conn, if_exists="replace", index=False)

        print("Corrigindo datas de cadastro de clientes...")
        df_cli = pd.read_sql("SELECT * FROM clientes", conn)
        if "data_cadastro" in df_cli.columns:
            df_cli["data_cadastro"] = _parse_datas(df_cli["data_cadastro"])

        if {"id_cliente", "data_pedido"}.issubset(df_ped.columns) and {
            "id_cliente",
            "data_cadastro",
        }.issubset(df_cli.columns):
            primeiro_pedido = (
                df_ped.dropna(subset=["data_pedido"])
                .groupby("id_cliente", as_index=False)["data_pedido"]
                .min()
                .rename(columns={"data_pedido": "primeiro_pedido"})
            )

            df_cli = df_cli.merge(primeiro_pedido, on="id_cliente", how="left")
            mascara_ajuste = df_cli["data_cadastro"].isna() | (
                df_cli["primeiro_pedido"].notna()
                & (df_cli["data_cadastro"] > df_cli["primeiro_pedido"])
            )
            df_cli.loc[mascara_ajuste, "data_cadastro"] = df_cli.loc[
                mascara_ajuste, "primeiro_pedido"
            ]
            df_cli = df_cli.drop(columns=["primeiro_pedido"])

        df_cli.to_sql("clientes_silver", conn, if_exists="replace", index=False)

    print("Processo de tratamento concluido.")


if __name__ == "__main__":
    processar_silver()