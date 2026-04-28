import sqlite3
from collections import Counter
from itertools import combinations
from pathlib import Path

import altair as alt
from typing import Dict, Optional, List, Any
import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).parent.parent
DB_PATH = ROOT_DIR / "data" / "processed" / "delivery_database.db"

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO DE PÁGINA E ESTILOS
# -----------------------------------------------------------------------------
st.set_page_config(page_title="MBA Analytics", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <style>
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    .kpi-card { background-color: #ffffff; border: 1px solid #e5e7eb; border-radius: 6px; padding: 1.25rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .kpi-title { font-size: 0.75rem; font-weight: 600; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .kpi-value { font-size: 1.875rem; font-weight: 700; color: #111827; line-height: 1.2; }
    .kpi-sub { font-size: 0.875rem; color: #059669; margin-top: 0.25rem; }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# MOTOR DE DADOS
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def extrair_dados(db_signature=None) -> dict[str, pd.DataFrame] | None:
    if not DB_PATH.exists():
        return None
    
    queries = {
        "clientes": "SELECT * FROM clientes_silver",
        "pedidos": "SELECT * FROM pedidos_silver",
        "produtos": "SELECT * FROM produtos_silver",
        "itens": "SELECT * FROM itens_pedido_silver"
    }
    
    tabelas = {}
    with sqlite3.connect(DB_PATH) as conn:
        for nome, query in queries.items():
            try:
                tabelas[nome] = pd.read_sql(query, conn)
            except Exception:
                tabelas[nome] = pd.DataFrame()
    return tabelas

@st.cache_data(show_spinner=False)
def construir_modelo_analitico(dados: Optional[Dict[str, pd.DataFrame]]) -> pd.DataFrame:
    if dados is None or dados["itens"].empty or dados["pedidos"].empty:
        return pd.DataFrame()

    df = dados["itens"].merge(dados["pedidos"], on="id_pedido", how="inner")
    
    if not dados["produtos"].empty:
        df = df.merge(dados["produtos"], on="id_produto", how="left")
        
    if not dados["clientes"].empty and "id_cliente" in df.columns:
        df = df.merge(dados["clientes"], on="id_cliente", how="left")
    
    colunas_financeiras = ["preco_unitario", "valor_total", "quantidade"]
    for col in colunas_financeiras:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "receita_linha" not in df.columns:
        if "quantidade" in df.columns and "preco_unitario" in df.columns:
            df["receita_linha"] = df["quantidade"] * df["preco_unitario"]
        elif "valor_total" in df.columns:
            df["receita_linha"] = df["valor_total"]
        else:
            df["receita_linha"] = 0.0

    return df


@st.cache_data(show_spinner=False)
def minerar_regras_associacao(df: pd.DataFrame, min_support: int=2) -> pd.DataFrame:
    col_prod = next((c for c in ["nome_produto", "produto", "descricao"] if c in df.columns), None)
    if not col_prod or df.empty:
        return pd.DataFrame()

    # Mapeamento de transações
    transacoes = df.groupby('id_pedido')[col_prod].apply(lambda x: list(set(x.dropna())))
    transacoes = transacoes[transacoes.map(len) > 1]
    
    total_transacoes = len(transacoes)
    if total_transacoes == 0:
        return pd.DataFrame()

    # Contagem de frequências
    freq_individual = Counter(item for sublist in transacoes for item in sublist)
    freq_pares = Counter(comb for sublist in transacoes for comb in combinations(sorted(sublist), 2))

    regras = []
    for (item_a, item_b), freq_ab in freq_pares.items():
        if freq_ab >= min_support:
            sup_ab = freq_ab / total_transacoes
            conf_a_b = freq_ab / freq_individual[item_a]
            conf_b_a = freq_ab / freq_individual[item_b]
            
            regras.append({"Antecedente": item_a, "Consequente": item_b, "Transações Conjuntas": freq_ab, "Suporte (%)": sup_ab * 100, "Confiança (%)": conf_a_b * 100})
            regras.append({"Antecedente": item_b, "Consequente": item_a, "Transações Conjuntas": freq_ab, "Suporte (%)": sup_ab * 100, "Confiança (%)": conf_b_a * 100})

    df_regras = pd.DataFrame(regras)
    if not df_regras.empty:
        df_regras = df_regras.sort_values(by="Confiança (%)", ascending=False).drop_duplicates(subset=["Antecedente", "Consequente"])
    
    return df_regras

# -----------------------------------------------------------------------------
# COMPONENTES VISUAIS
# -----------------------------------------------------------------------------
def renderizar_kpi(titulo: str, valor: str, prefixo: str ="") -> None:
    html = f"""
    <div class="kpi-card">
        <div class="kpi-title">{titulo}</div>
        <div class="kpi-value">{prefixo}{valor}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# EXECUÇÃO PRINCIPAL
# -----------------------------------------------------------------------------
db_signature = DB_PATH.stat().st_mtime_ns if DB_PATH.exists() else None
dados_brutos = extrair_dados(db_signature)
if not dados_brutos:
    st.error(f"Falha de I/O: Banco de dados não localizado em {DB_PATH}")
    st.stop()

df_master = construir_modelo_analitico(dados_brutos)

st.title("Market Basket Insights")
st.markdown("---")

if df_master.empty:
    st.warning("Dataframe mestre vazio. Verifique a integridade das tabelas silver.")
    st.stop()

# Filtros Globais
col_filtros = st.columns(3)

# Filtro de Status / Faturamento
col_status = next((c for c in ["status", "status_pedido", "situacao", "estado_pedido"] if c in df_master.columns), None)
if col_status:
    status_base = df_master[col_status].dropna().astype(str).str.strip()
    status_disp = sorted(status_base.unique().tolist())
    status_sel = col_filtros[0].multiselect(
        "Faturamento (Status)",
        options=status_disp,
        default=status_disp,
        help="Selecione um ou mais status para recalcular KPIs e análises.",
        key="filtro_status_faturamento",
    )
    if status_sel:
        df_master = df_master[df_master[col_status].astype(str).str.strip().isin(status_sel)]
    
    # Identificar se é "Todos" para o título dos KPIs
    selecao_completa = len(status_sel) == len(status_disp) or len(status_sel) == 0
    label_status = "Todos os status" if selecao_completa else ", ".join(status_sel)

col_estado = next((c for c in ["estado", "uf"] if c in df_master.columns), None)
if col_estado:
    estados_disp = sorted(df_master[col_estado].dropna().astype(str).unique())
    estados_sel = col_filtros[1].multiselect("Estado", estados_disp, default=[])
    if estados_sel:
        df_master = df_master[df_master[col_estado].isin(estados_sel)]

col_cat = next((c for c in ["categoria", "segmento"] if c in df_master.columns), None)
if col_cat:
    cat_disp = sorted(df_master[col_cat].dropna().astype(str).unique())
    cat_sel = col_filtros[2].multiselect("Categorias", cat_disp, default=[])
    if cat_sel:
        df_master = df_master[df_master[col_cat].isin(cat_sel)]

# Métricas Top-Level
pedidos_unicos = df_master["id_pedido"].nunique()
receita_total = df_master["receita_linha"].sum()
itens_vendidos = len(df_master)
ticket_medio = receita_total / pedidos_unicos if pedidos_unicos > 0 else 0

# Títulos dinâmicos baseados no filtro
label_pedidos = f"Pedidos ({label_status})"
label_receita = f"Receita {label_status}"

m1, m2, m3, m4 = st.columns(4)
with m1: renderizar_kpi(label_pedidos, f"{pedidos_unicos:,.0f}".replace(",", "."))
with m2: renderizar_kpi(label_receita, f"{receita_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), "R$ ")
with m3: renderizar_kpi("Ticket Médio", f"{ticket_medio:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), "R$ ")
with m4: renderizar_kpi("Itens por Cesta", f"{itens_vendidos / pedidos_unicos if pedidos_unicos else 0:.2f}")

st.markdown("<br>", unsafe_allow_html=True)

# Abas de Análise
aba_mba, aba_temporal, aba_qualidade = st.tabs(["Market Basket Analysis", "Série Temporal", "Data Quality"])

with aba_mba:
    # --- Contribuição Graeff - Explicação de Termos Técnicos ---
    with st.expander("Entenda os conceitos desta análise"):
        st.markdown("""
        Para interpretar as sugestões de compra casada, considere:
        
        * **Antecedente (Produto A):** O item que o cliente já possui ou adicionou ao carrinho.
        * **Consequente (Produto B):** O item sugerido para cross-sell (venda relacionada).
        * **Confiança:** Indica a força da relação. Se a confiança é **70%**, significa que em 7 em cada 10 vezes que o Produto A foi comprado, o Produto B também estava no pedido.
        * **Suporte:** Indica a popularidade da combinação no total de vendas.
        """)

    df_regras = minerar_regras_associacao(df_master)
    
    if df_regras.empty:
        st.info("Volume transacional insuficiente para extração de regras de associação nesta amostra.")
    else:
        c1, c2 = st.columns([1.5, 1])
        
        with c1:
            heatmap = alt.Chart(df_regras.head(30)).mark_rect().encode(
                x=alt.X('Consequente:N', title="Produto B (Comprado junto)"),
                y=alt.Y('Antecedente:N', title="Produto A (Item Base)", sort='-color'),
                color=alt.Color('Confiança (%):Q', scale=alt.Scale(scheme='tealblues'), title="Confiança (%)"),
                tooltip=['Antecedente', 'Consequente', 'Transações Conjuntas', alt.Tooltip('Confiança (%):Q', format='.1f')]
            ).properties(height=450, title="Matriz de Confiança de Cross-Sell")
            st.altair_chart(heatmap, use_container_width=True)
            
        with c2:
            st.markdown("##### Top Regras de Associação")
            st.dataframe(
                df_regras[['Antecedente', 'Consequente', 'Confiança (%)']].head(10).style.format({"Confiança (%)": "{:.1f}%"}),
                hide_index=True, use_container_width=True
            )

with aba_temporal:
    col_data = next((c for c in ["data_pedido", "data_compra"] if c in df_master.columns), None)
    if col_data:
        df_master['mes_ano'] = pd.to_datetime(df_master[col_data]).dt.to_period('M').dt.to_timestamp()
        df_tendencia = df_master.groupby('mes_ano').agg({'receita_linha': 'sum', 'id_pedido': 'nunique'}).reset_index()
        
        linha_tendencia = alt.Chart(df_tendencia).mark_line(point=True, color="#1f77b4").encode(
            x=alt.X('mes_ano:T', title="Período"),
            y=alt.Y('receita_linha:Q', title="Receita (R$)"),
            tooltip=['mes_ano:T', 'receita_linha:Q', 'id_pedido:Q']
        ).properties(height=350, title="Evolução de Receita")
        st.altair_chart(linha_tendencia, use_container_width=True)
    else:
        st.warning("Coluna de data não identificada no schema.")

with aba_qualidade:
    tabelas = ["clientes", "pedidos", "produtos", "itens"]
    dados_qa = []
    
    for tab in tabelas:
        df_tab = dados_brutos.get(tab, pd.DataFrame())
        if not df_tab.empty:
            nulos = df_tab.isna().sum().sum()
            total_celulas = df_tab.size
            tx_preenchimento = 100 - ((nulos / total_celulas) * 100) if total_celulas > 0 else 0
            dados_qa.append({"Tabela (Silver)": tab, "Linhas": len(df_tab), "Taxa de Preenchimento": f"{tx_preenchimento:.2f}%", "Valores Nulos": nulos})
            
    st.table(pd.DataFrame(dados_qa))
    