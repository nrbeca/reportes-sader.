"""
SADER - Sistema de Reportes Presupuestarios
Aplicacion Streamlit para procesar archivos MAP y SICOP
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import io
import base64

# Importar modulos propios
from config import (
    MONTH_NAMES_FULL, formatear_fecha, obtener_ultimo_dia_habil, get_config_by_year
)
from map_processor import procesar_map
from sicop_processor import procesar_sicop
from excel_map import generar_excel_map
from excel_sicop import generar_excel_sicop

# ============================================================================
# CONFIGURACION DE PAGINA
# ============================================================================

st.set_page_config(
    page_title="SADER - Reportes Presupuestarios",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# ESTILOS CSS
# ============================================================================

st.markdown("""
<style>
    /* Colores institucionales SADER */
    :root {
        --sader-vino: #9B2247;
        --sader-beige: #E6D194;
        --sader-gris: #98989A;
        --sader-verde: #002F2A;
        --sader-blanco: #FFFFFF;
    }
    
    /* Fondo general blanco */
    .stApp {
        background-color: #FFFFFF;
    }
    
    /* Header */
    .main-header {
        background: linear-gradient(135deg, #9B2247 0%, #7a1b38 100%);
        color: white;
        padding: 1.5rem 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2rem;
        font-weight: 600;
        color: white;
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        opacity: 0.9;
        font-size: 1rem;
        color: white;
    }
    
    /* Tarjetas KPI */
    .kpi-card {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border-left: 4px solid #9B2247;
        transition: transform 0.2s;
    }
    
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(155, 34, 71, 0.15);
    }
    
    .kpi-label {
        font-size: 0.85rem;
        color: #333;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }
    
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #9B2247;
    }
    
    .kpi-subtitle {
        font-size: 0.8rem;
        color: #666;
        margin-top: 0.25rem;
    }
    
    /* Upload area */
    .upload-zone {
        border: 2px dashed #E6D194;
        border-radius: 12px;
        padding: 2rem;
        text-align: center;
        background: #fafafa;
        transition: all 0.3s;
    }
    
    .upload-zone:hover {
        border-color: #9B2247;
        background: rgba(155, 34, 71, 0.02);
    }
    
    /* Instrucciones visibles */
    .instrucciones-box {
        background: #f8f8f8;
        border: 1px solid #E6D194;
        border-radius: 10px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }
    
    .instrucciones-box h4 {
        color: #9B2247;
        margin-top: 0;
    }
    
    .instrucciones-box ol {
        margin-bottom: 0;
        color: #333;
    }
    
    /* Sidebar con fondo vino */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #9B2247 0%, #7a1b38 100%);
    }
    
    section[data-testid="stSidebar"] .stMarkdown {
        color: white;
    }
    
    section[data-testid="stSidebar"] p, 
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] span {
        color: white !important;
    }
    
    /* Tablas */
    .dataframe {
        font-size: 0.9rem;
    }
    
    /* Botones principales - vino */
    .stButton > button {
        background: linear-gradient(135deg, #9B2247 0%, #7a1b38 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-weight: 500;
        transition: all 0.2s;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(155, 34, 71, 0.3);
    }
    
    /* Boton de descarga - beige con letras negras */
    .stDownloadButton > button {
        background: linear-gradient(135deg, #E6D194 0%, #d4bc7a 100%);
        color: #000;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: all 0.2s;
    }
    
    .stDownloadButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(230, 209, 148, 0.5);
    }
    
    /* Tabs personalizados */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: #f8f8f8;
        padding: 0.5rem;
        border-radius: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 8px;
        color: #333;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: #9B2247 !important;
        color: white !important;
    }
    
    /* Metricas */
    [data-testid="stMetricValue"] {
        color: #9B2247;
        font-weight: 700;
    }
    
    [data-testid="stMetricLabel"] {
        color: #333;
    }
    
    /* Encabezados */
    h1, h2, h3, h4 {
        color: #9B2247;
    }
    
    /* Links */
    a {
        color: #9B2247;
    }
    
    a:hover {
        color: #7a1b38;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def format_currency(value):
    """Formatea un numero como moneda mexicana"""
    if pd.isna(value) or value == 0:
        return "$0.00"
    return f"${value:,.2f}"

def format_currency_millions(value):
    """Formatea un numero en millones"""
    if pd.isna(value) or value == 0:
        return "$0.00 M"
    return f"${value/1_000_000:,.2f} M"

def format_percentage(value):
    """Formatea un numero como porcentaje"""
    if pd.isna(value):
        return "0.00%"
    return f"{value*100:.2f}%"

def create_kpi_card(label, value, subtitle="", bg_color=None):
    """Crea una tarjeta KPI con colores apropiados"""
    if bg_color:
        # Si hay color de fondo
        if bg_color == '#98989A' or bg_color == '#E6D194':
            # Gris o beige = letras negras
            text_color = '#000'
        else:
            # Otros colores (vino, verde) = letras blancas
            text_color = '#fff'
        return f"""
        <div style="background: {bg_color}; border-radius: 12px; padding: 1.5rem; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
            <div style="font-size: 0.85rem; color: {text_color}; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.5rem;">{label}</div>
            <div style="font-size: 1.8rem; font-weight: 700; color: {text_color};">{value}</div>
            <div style="font-size: 0.8rem; color: {text_color}; opacity: 0.9; margin-top: 0.25rem;">{subtitle}</div>
        </div>
        """
    else:
        # Sin color de fondo = fondo blanco, letras oscuras
        return f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-subtitle">{subtitle}</div>
        </div>
        """

# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0; margin-bottom: 1rem; border-bottom: 1px solid rgba(255,255,255,0.2);">
        <div style="background: rgba(255,255,255,0.1); 
                    padding: 1rem; border-radius: 10px; color: white; font-weight: bold; font-size: 1.5rem;">
            SADER
        </div>
        <p style="color: rgba(255,255,255,0.8); font-size: 0.8rem; margin-top: 0.5rem;">
            Sistema de Reportes
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Tipo de Reporte")
    reporte_tipo = st.radio(
        "Selecciona el reporte a generar:",
        ["MAP - Cuadro de presupuesto", "SICOP - Estado del Ejercicio"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    st.markdown("### Configuracion")
    
    # Info de fecha
    hoy = date.today()
    ultimo_habil = obtener_ultimo_dia_habil(hoy)
    
    st.markdown(f"**Fecha actual:** {formatear_fecha(hoy)}")
    st.caption(f"Ultimo dia habil: {formatear_fecha(ultimo_habil)}")

# ============================================================================
# CONTENIDO PRINCIPAL
# ============================================================================

# Header
st.markdown("""
<div class="main-header">
    <h1>Sistema de Reportes Presupuestarios</h1>
    <p>Secretaria de Agricultura y Desarrollo Rural</p>
</div>
""", unsafe_allow_html=True)

# Determinar que procesador usar
es_map = "MAP" in reporte_tipo

# Layout: Instrucciones al lado del upload
col_upload, col_instrucciones = st.columns([2, 1])

with col_upload:
    st.markdown(f"### {'MAP' if es_map else 'SICOP'} - Cargar Archivo")
    
    uploaded_file = st.file_uploader(
        "Arrastra tu archivo CSV aqui o haz clic para seleccionar",
        type=['csv'],
        help="Sube el archivo CSV exportado del sistema correspondiente"
    )

with col_instrucciones:
    st.markdown("""
    <div class="instrucciones-box">
        <h4>Instrucciones</h4>
        <ol>
            <li>Selecciona el tipo de reporte en el menu lateral</li>
            <li>Sube el archivo CSV correspondiente</li>
            <li>Revisa los resultados</li>
            <li>Descarga el Excel</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)

if uploaded_file is not None:
    # Leer archivo
    try:
        df = pd.read_csv(uploaded_file, encoding='latin-1', low_memory=False)
        filename = uploaded_file.name
        
        st.success(f"Archivo cargado: **{filename}** ({len(df):,} registros)")
        
        # Procesar segun tipo
        with st.spinner("Procesando datos..."):
            if es_map:
                resultados = procesar_map(df, filename)
            else:
                resultados = procesar_sicop(df, filename)
        
        metadata = resultados['metadata']
        config = metadata['config']
        
        # Info del archivo
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.metric("Fecha del archivo", formatear_fecha(metadata['fecha_archivo']))
        with col_info2:
            st.metric("Mes", MONTH_NAMES_FULL[metadata['mes'] - 1])
        with col_info3:
            año_config = "2026 (Nuevos)" if config['usar_2026'] else "2025 (Anteriores)"
            st.metric("Configuracion", año_config)
        
        st.markdown("---")
        
        # ============================================================================
        # RESULTADOS MAP
        # ============================================================================
        
        if es_map:
            st.markdown("### Resumen Presupuestario")
            
            totales = resultados['totales']
            
            # KPIs principales
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(create_kpi_card(
                    "PEF Original",
                    format_currency_millions(totales['Original']),
                    "Presupuesto aprobado"
                ), unsafe_allow_html=True)
            
            with col2:
                st.markdown(create_kpi_card(
                    "Modificado Anual",
                    format_currency_millions(totales['ModificadoAnualNeto']),
                    "Neto de congelados",
                    "#9B2247"
                ), unsafe_allow_html=True)
            
            with col3:
                st.markdown(create_kpi_card(
                    "Modificado Periodo",
                    format_currency_millions(totales['ModificadoPeriodoNeto']),
                    f"Al mes de {MONTH_NAMES_FULL[metadata['mes'] - 1]}",
                    "#E6D194"
                ), unsafe_allow_html=True)
            
            with col4:
                st.markdown(create_kpi_card(
                    "Ejercido",
                    format_currency_millions(totales['Ejercido']),
                    format_percentage(totales['Ejercido'] / totales['ModificadoPeriodoNeto'] if totales['ModificadoPeriodoNeto'] > 0 else 0) + " avance",
                    "#98989A"
                ), unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Tabs de visualizacion
            tab1, tab2, tab3 = st.tabs(["Por Seccion", "Detalle Programas", "Graficas"])
            
            # Preparar datos para tabla
            categorias = resultados['categorias']
            cat_data = []
            for cat_key, cat_name in [
                ('servicios_personales', 'Servicios Personales'),
                ('gasto_corriente', 'Gasto Corriente'),
                ('subsidios', 'Subsidios y Gastos asociados'),
                ('otros_programas', 'Otros programas'),
                ('bienes_muebles', 'Bienes muebles e intangibles')
            ]:
                if cat_key in categorias:
                    datos = categorias[cat_key]
                    disponible = datos['ModificadoPeriodoNeto'] - datos['Ejercido']
                    pct = datos['Ejercido'] / datos['ModificadoPeriodoNeto'] * 100 if datos['ModificadoPeriodoNeto'] > 0 else 0
                    cat_data.append({
                        'Categoria': cat_name,
                        'Original': datos['Original'],
                        'Mod. Anual': datos['ModificadoAnualNeto'],
                        'Mod. Periodo': datos['ModificadoPeriodoNeto'],
                        'Ejercido': datos['Ejercido'],
                        'Disponible': disponible,
                        '% Avance': pct
                    })
            
            df_cat = pd.DataFrame(cat_data)
            
            with tab1:
                st.dataframe(
                    df_cat.style.format({
                        'Original': '${:,.2f}',
                        'Mod. Anual': '${:,.2f}',
                        'Mod. Periodo': '${:,.2f}',
                        'Ejercido': '${:,.2f}',
                        'Disponible': '${:,.2f}',
                        '% Avance': '{:.2f}%'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
            
            with tab2:
                programas = resultados['programas']
                prog_nombres = config['programas_nombres']
                prog_data = []
                
                for prog, datos in programas.items():
                    if datos['Original'] > 0 or datos['ModificadoAnualNeto'] > 0:
                        disponible = datos['ModificadoPeriodoNeto'] - datos['Ejercido']
                        pct = datos['Ejercido'] / datos['ModificadoPeriodoNeto'] * 100 if datos['ModificadoPeriodoNeto'] > 0 else 0
                        prog_data.append({
                            'Programa': prog,
                            'Nombre': prog_nombres.get(prog, prog)[:50] + '...' if len(prog_nombres.get(prog, prog)) > 50 else prog_nombres.get(prog, prog),
                            'Original': datos['Original'],
                            'Mod. Anual': datos['ModificadoAnualNeto'],
                            'Mod. Periodo': datos['ModificadoPeriodoNeto'],
                            'Ejercido': datos['Ejercido'],
                            '% Avance': pct
                        })
                
                df_prog = pd.DataFrame(prog_data)
                
                st.dataframe(
                    df_prog.style.format({
                        'Original': '${:,.2f}',
                        'Mod. Anual': '${:,.2f}',
                        'Mod. Periodo': '${:,.2f}',
                        'Ejercido': '${:,.2f}',
                        '% Avance': '{:.2f}%'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
            
            with tab3:
                col_g1, col_g2 = st.columns(2)
                
                with col_g1:
                    st.markdown("#### Distribucion por Categoria")
                    
                    fig_pie = px.pie(
                        df_cat,
                        values='Mod. Periodo',
                        names='Categoria',
                        color_discrete_sequence=['#9B2247', '#E6D194', '#98989A', '#002F2A', '#4a4a4a']
                    )
                    fig_pie.update_layout(
                        showlegend=True,
                        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
                        margin=dict(t=20, b=20, l=20, r=20)
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                with col_g2:
                    st.markdown("#### Avance de Ejercicio")
                    
                    fig_bar = go.Figure()
                    fig_bar.add_trace(go.Bar(
                        name='Ejercido',
                        x=df_cat['Categoria'],
                        y=df_cat['Ejercido'],
                        marker_color='#9B2247'
                    ))
                    fig_bar.add_trace(go.Bar(
                        name='Disponible',
                        x=df_cat['Categoria'],
                        y=df_cat['Disponible'],
                        marker_color='#002F2A'
                    ))
                    fig_bar.update_layout(
                        barmode='stack',
                        xaxis_tickangle=-45,
                        margin=dict(t=20, b=100, l=20, r=20),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)
        
        # ============================================================================
        # RESULTADOS SICOP
        # ============================================================================
        
        else:
            st.markdown("### Resumen por Unidad Responsable")
            
            totales = resultados['totales']
            
            # KPIs principales
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(create_kpi_card(
                    "Original",
                    format_currency_millions(totales['Original']),
                    "Presupuesto aprobado"
                ), unsafe_allow_html=True)
            
            with col2:
                st.markdown(create_kpi_card(
                    "Modificado Anual",
                    format_currency_millions(totales['Modificado_anual']),
                    "Neto de congelados",
                    "#9B2247"
                ), unsafe_allow_html=True)
            
            with col3:
                st.markdown(create_kpi_card(
                    "Ejercido Acumulado",
                    format_currency_millions(totales['Ejercido_acumulado']),
                    "Ejercido + Devengado + Tramite",
                    "#E6D194"
                ), unsafe_allow_html=True)
            
            with col4:
                pct_avance = totales['Pct_avance_periodo'] * 100 if totales['Pct_avance_periodo'] else 0
                st.markdown(create_kpi_card(
                    "Avance al Periodo",
                    f"{pct_avance:.2f}%",
                    f"Meta: {metadata['mes'] / 12 * 100:.1f}%",
                    "#98989A"
                ), unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Tabs de visualizacion
            tab1, tab2, tab3 = st.tabs(["Por Seccion", "Detalle URs", "Graficas"])
            
            with tab1:
                subtotales = resultados['subtotales']
                
                seccion_data = []
                for seccion_key, seccion_name in [
                    ('sector_central', 'Sector Central'),
                    ('oficinas', 'Oficinas de Representacion'),
                    ('organos_desconcentrados', 'Organos Desconcentrados'),
                    ('entidades_paraestatales', 'Entidades Paraestatales')
                ]:
                    if seccion_key in subtotales:
                        datos = subtotales[seccion_key]
                        pct = datos['Pct_avance_periodo'] * 100 if datos.get('Pct_avance_periodo') else 0
                        seccion_data.append({
                            'Seccion': seccion_name,
                            'Original': datos['Original'],
                            'Mod. Anual': datos['Modificado_anual'],
                            'Mod. Periodo': datos['Modificado_periodo'],
                            'Ejercido': datos['Ejercido_acumulado'],
                            'Disponible': datos['Disponible_periodo'],
                            '% Avance': pct
                        })
                
                df_seccion = pd.DataFrame(seccion_data)
                
                st.dataframe(
                    df_seccion.style.format({
                        'Original': '${:,.2f}',
                        'Mod. Anual': '${:,.2f}',
                        'Mod. Periodo': '${:,.2f}',
                        'Ejercido': '${:,.2f}',
                        'Disponible': '${:,.2f}',
                        '% Avance': '{:.2f}%'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
            
            with tab2:
                # Selector de UR
                resumen = resultados['resumen']
                denominaciones = config['denominaciones']
                
                # Crear lista de URs con su denominacion
                urs_disponibles = resumen['UR'].tolist()
                urs_con_nombre = [f"{ur} - {denominaciones.get(ur, 'Sin nombre')[:40]}" for ur in urs_disponibles]
                
                ur_seleccionada = st.selectbox(
                    "Selecciona una Unidad Responsable:",
                    options=urs_con_nombre,
                    index=0
                )
                
                # Obtener datos de la UR seleccionada
                ur_codigo = ur_seleccionada.split(" - ")[0]
                datos_ur = resumen[resumen['UR'] == ur_codigo].iloc[0]
                
                st.markdown(f"#### {denominaciones.get(ur_codigo, ur_codigo)}")
                
                # Mostrar datos de la UR
                col_ur1, col_ur2, col_ur3, col_ur4 = st.columns(4)
                
                with col_ur1:
                    st.metric("Original", format_currency(datos_ur['Original']))
                with col_ur2:
                    st.metric("Modificado Anual", format_currency(datos_ur['Modificado_anual']))
                with col_ur3:
                    st.metric("Ejercido", format_currency(datos_ur['Ejercido_acumulado']))
                with col_ur4:
                    pct = datos_ur['Pct_avance_periodo'] * 100 if datos_ur['Pct_avance_periodo'] else 0
                    st.metric("Avance", f"{pct:.2f}%")
                
                # Tabla completa de la UR
                st.markdown("**Detalle completo:**")
                df_ur_display = pd.DataFrame([{
                    'Concepto': 'Valores',
                    'Original': datos_ur['Original'],
                    'Mod. Anual': datos_ur['Modificado_anual'],
                    'Mod. Periodo': datos_ur['Modificado_periodo'],
                    'Ejercido': datos_ur['Ejercido_acumulado'],
                    'Disp. Anual': datos_ur['Disponible_anual'],
                    'Disp. Periodo': datos_ur['Disponible_periodo'],
                    '% Anual': datos_ur['Pct_avance_anual'] * 100 if datos_ur['Pct_avance_anual'] else 0,
                    '% Periodo': datos_ur['Pct_avance_periodo'] * 100 if datos_ur['Pct_avance_periodo'] else 0
                }])
                
                st.dataframe(
                    df_ur_display.style.format({
                        'Original': '${:,.2f}',
                        'Mod. Anual': '${:,.2f}',
                        'Mod. Periodo': '${:,.2f}',
                        'Ejercido': '${:,.2f}',
                        'Disp. Anual': '${:,.2f}',
                        'Disp. Periodo': '${:,.2f}',
                        '% Anual': '{:.2f}%',
                        '% Periodo': '{:.2f}%'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
            
            with tab3:
                col_g1, col_g2 = st.columns(2)
                
                with col_g1:
                    st.markdown("#### Distribucion por Seccion")
                    
                    fig_pie = px.pie(
                        df_seccion,
                        values='Mod. Periodo',
                        names='Seccion',
                        color_discrete_sequence=['#9B2247', '#E6D194', '#98989A', '#002F2A']
                    )
                    fig_pie.update_layout(
                        showlegend=True,
                        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
                        margin=dict(t=20, b=20, l=20, r=20)
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                with col_g2:
                    st.markdown("#### Avance por Seccion")
                    
                    fig_bar = go.Figure()
                    fig_bar.add_trace(go.Bar(
                        name='Ejercido',
                        x=df_seccion['Seccion'],
                        y=df_seccion['Ejercido'],
                        marker_color='#9B2247'
                    ))
                    fig_bar.add_trace(go.Bar(
                        name='Disponible',
                        x=df_seccion['Seccion'],
                        y=df_seccion['Disponible'],
                        marker_color='#002F2A'
                    ))
                    fig_bar.update_layout(
                        barmode='stack',
                        xaxis_tickangle=-45,
                        margin=dict(t=20, b=100, l=20, r=20),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)
        
        # ============================================================================
        # DESCARGA - Solo Excel
        # ============================================================================
        
        st.markdown("---")
        
        # Generar Excel formateado
        if es_map:
            excel_bytes = generar_excel_map(resultados)
            fecha_str = date.today().strftime('%d%b%Y').upper()
            config_str = "Prog2026" if config['usar_2026'] else "Prog2025"
            filename_excel = f'Cuadro_Presupuesto_{config_str}_{fecha_str}.xlsx'
        else:
            excel_bytes = generar_excel_sicop(resultados)
            fecha_str = date.today().strftime('%d%b%Y').upper()
            config_str = "URs2026" if config['usar_2026'] else "URs2025"
            filename_excel = f'Estado_Ejercicio_SICOP_{config_str}_{fecha_str}.xlsx'
        
        st.download_button(
            label="Descargar Excel",
            data=excel_bytes,
            file_name=filename_excel,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
    except Exception as e:
        st.error(f"Error al procesar el archivo: {str(e)}")
        st.exception(e)

else:
    # Estado inicial - sin archivo
    st.markdown("""
    <div class="upload-zone">
        <h3>Sube tu archivo CSV</h3>
        <p style="color: #666;">Arrastra y suelta o haz clic en el boton de arriba</p>
        <p style="color: #888; font-size: 0.9rem; margin-top: 1rem;">
            Formatos soportados: CSV exportado de MAP o SICOP
        </p>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #888; font-size: 0.8rem;">
    <p>SADER - Sistema de Reportes Presupuestarios | Unidad de Administracion y Finanzas</p>
</div>
""", unsafe_allow_html=True)
