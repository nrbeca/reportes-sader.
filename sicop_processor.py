# ============================================================================
# PROCESADOR DE ARCHIVOS SICOP
# ============================================================================

import pandas as pd
import numpy as np
from datetime import date
from config import (
    MONTH_NAMES, round_like_excel, detectar_fecha_archivo,
    get_config_by_year, numero_a_letras_mx
)


def obtener_columnas_hasta_mes(mes_numero):
    """Obtiene las columnas de modificaciones y reservas hasta el mes indicado"""
    todos_los_meses = [
        ('EN', 'ENE'), ('FE', 'FEB'), ('MR', 'MZO'), ('AB', 'ABR'),
        ('MY', 'MAY'), ('JN', 'JUN'), ('JL', 'JUL'), ('AG', 'AGO'),
        ('SE', 'SEP'), ('OC', 'OCT'), ('NO', 'NOV'), ('DI', 'DIC')
    ]
    meses_usar = todos_los_meses[:mes_numero]
    return {
        'modificaciones': [f'MO{abrev}' for abrev, _ in meses_usar],
        'reservas': [f'RESERVA_{completo}' for _, completo in meses_usar],
    }


def calcular_congelado_anual(df):
    """Calcula el total de recursos congelados en el año"""
    todos_meses = ['ENE', 'FEB', 'MZO', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    cols = [f'RESERVA_{mes}' for mes in todos_meses if f'RESERVA_{mes}' in df.columns]
    if cols:
        return round_like_excel(df[cols].sum(axis=1).sum(), 2)
    return 0


def calcular_congelado_periodo(df, mes_numero):
    """Calcula el total de recursos congelados hasta el mes indicado"""
    cols_a_usar = obtener_columnas_hasta_mes(mes_numero)
    cols = [col for col in cols_a_usar['reservas'] if col in df.columns]
    if cols:
        return round_like_excel(df[cols].sum(axis=1).sum(), 2)
    return 0


def mapear_ur(id_unidad, config):
    """Mapea una UR original a la UR correspondiente según el año"""
    id_str = str(id_unidad)
    mapeo_base = config['mapeo_ur']
    fusion_urs = config.get('fusion_urs', {})
    
    # Primero aplicar mapeo base
    if id_unidad in mapeo_base:
        id_str = str(mapeo_base[id_unidad])
    elif id_str.isdigit() and int(id_str) in mapeo_base:
        id_str = str(mapeo_base[int(id_str)])
    
    # Luego aplicar fusión si es 2026
    if config['usar_2026'] and id_str in fusion_urs:
        return fusion_urs[id_str]
    
    return id_str


def procesar_sicop(df, filename):
    """
    Procesa el archivo SICOP y devuelve los resultados calculados.
    
    Returns:
        dict con:
        - 'resumen': DataFrame con totales por UR
        - 'subtotales': dict con subtotales por sección
        - 'congelados': dict con congelados anual y periodo
        - 'totales': dict con totales generales
        - 'metadata': información del archivo
    """
    # Detectar fecha y configuración
    fecha_archivo, mes_archivo, año_archivo = detectar_fecha_archivo(filename)
    config = get_config_by_year(año_archivo)
    
    año_actual = date.today().year
    es_cierre_año_anterior = (mes_archivo in [1, 2]) and (año_archivo < año_actual)
    
    # Aplicar mapeo de URs
    df['ID_UNIDAD'] = df['ID_UNIDAD'].astype(str)
    df['Nueva UR'] = df['ID_UNIDAD'].apply(lambda x: mapear_ur(x, config))
    
    # Calcular Partida
    df['Partida'] = (
        df['CAPITULO'] * 10000 + df['CONCEPTO'] * 1000 +
        df['PARTIDA_GENERICA'] * 100 + df['PARTIDA_ESPECIFICA'] * 10
    ).astype(int)
    
    # Calcular EJERCIDO_REAL
    for col in ['EJERCIDO', 'DEVENGADO', 'EJERCIDO_TRAMITE']:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = df[col].fillna(0)
    
    df['EJERCIDO_REAL'] = df['EJERCIDO'] + df['DEVENGADO'] + df['EJERCIDO_TRAMITE']
    
    # URs válidas
    urs_validas = (config['sector_central'] + config['oficinas'] + 
                   config['organos_desconcentrados'] + config['entidades_paraestatales'])
    
    # Guardar copia para congelados antes de filtrar
    df_para_congelados = df.copy()
    
    # Aplicar filtros
    df = df[df['Nueva UR'].astype(str).isin(urs_validas)].copy()
    df = df[~df['Partida'].isin([39801, 39810])].copy()
    df = df[~df['CAPITULO'].isin([1, 7])].copy()
    df = df[df['CONTROL_OPERATIVO'].isin([0, 10, 40, 50, 51])].copy()
    
    # Calcular por UR
    resultados_ur = {}
    
    for ur in urs_validas:
        df_ur = df[df['Nueva UR'].astype(str) == ur].copy()
        
        if len(df_ur) == 0:
            resultados_ur[ur] = {
                'Original': 0, 'Modificado_anual': 0, 'Modificado_periodo': 0, 'Ejercido': 0
            }
            continue
        
        # Calcular Modificado neto
        df_ur['Modificado_neto'] = df_ur['MODIFICADO_AUTORIZADO'] - df_ur['RESERVAS']
        
        # ORIGINAL: Suma donde CO=0
        df_co0 = df_ur[df_ur['CONTROL_OPERATIVO'] == 0]
        original = round_like_excel(df_co0['ORIGINAL'].sum(), 2)
        
        # MODIFICADO: Filtros de CO según tipo de UR
        if ur in config['entidades_paraestatales'] or ur == 'RJL':
            df_modificado = df_ur[df_ur['CONTROL_OPERATIVO'].isin([0, 50])]
        elif ur in config['organos_desconcentrados']:
            df_modificado = df_ur[df_ur['CONTROL_OPERATIVO'].isin([0, 50])]
        else:
            df_modificado = df_ur[df_ur['CONTROL_OPERATIVO'].isin([0, 50, 51])]
        
        # MODIFICADO ANUAL
        modificado_anual = round_like_excel(df_modificado['Modificado_neto'].sum(), 2)
        
        # MODIFICADO PERIODO
        if es_cierre_año_anterior or mes_archivo == 12:
            modificado_periodo = modificado_anual
        else:
            cols_a_usar = obtener_columnas_hasta_mes(mes_archivo)
            cols_mod = [col for col in cols_a_usar['modificaciones'] if col in df_modificado.columns]
            cols_res = [col for col in cols_a_usar['reservas'] if col in df_modificado.columns]
            
            mod_bruto = df_modificado[cols_mod].sum(axis=1).sum() if cols_mod else 0
            cong_periodo = df_modificado[cols_res].sum(axis=1).sum() if cols_res else 0
            modificado_periodo = round_like_excel(mod_bruto - cong_periodo, 2)
        
        # EJERCIDO
        if ur in config['entidades_paraestatales'] or ur == 'RJL':
            df_ejercido = df_ur[df_ur['CONTROL_OPERATIVO'].isin([0, 50])]
        elif ur in config['organos_desconcentrados']:
            df_ejercido = df_ur[df_ur['CONTROL_OPERATIVO'].isin([0, 50])]
        else:
            df_ejercido = df_ur[df_ur['CONTROL_OPERATIVO'].isin([0, 50, 51])]
        
        ejercido = round_like_excel(df_ejercido['EJERCIDO_REAL'].sum(), 2)
        
        resultados_ur[ur] = {
            'Original': original,
            'Modificado_anual': modificado_anual,
            'Modificado_periodo': modificado_periodo,
            'Ejercido': ejercido
        }
    
    # Crear DataFrame de resumen
    resumen = pd.DataFrame.from_dict(resultados_ur, orient='index').reset_index()
    resumen.columns = ['UR', 'Original', 'Modificado_anual', 'Modificado_periodo', 'Ejercido_acumulado']
    
    # Calcular disponibles y porcentajes
    resumen['Disponible_anual'] = resumen.apply(
        lambda row: round_like_excel(row['Modificado_anual'] - row['Ejercido_acumulado'], 2), axis=1
    )
    resumen['Disponible_periodo'] = resumen.apply(
        lambda row: round_like_excel(row['Modificado_periodo'] - row['Ejercido_acumulado'], 2), axis=1
    )
    resumen['Pct_avance_anual'] = resumen.apply(
        lambda row: row['Ejercido_acumulado'] / row['Modificado_anual'] if row['Modificado_anual'] != 0 else 0, axis=1
    )
    resumen['Pct_avance_periodo'] = resumen.apply(
        lambda row: row['Ejercido_acumulado'] / row['Modificado_periodo'] if row['Modificado_periodo'] != 0 else 0, axis=1
    )
    
    # Calcular subtotales por sección
    def calcular_subtotal(urs_lista):
        df_seccion = resumen[resumen['UR'].isin(urs_lista)]
        subtotal = {
            'Original': df_seccion['Original'].sum(),
            'Modificado_anual': df_seccion['Modificado_anual'].sum(),
            'Modificado_periodo': df_seccion['Modificado_periodo'].sum(),
            'Ejercido_acumulado': df_seccion['Ejercido_acumulado'].sum(),
            'Disponible_anual': df_seccion['Disponible_anual'].sum(),
            'Disponible_periodo': df_seccion['Disponible_periodo'].sum(),
        }
        subtotal['Pct_avance_anual'] = subtotal['Ejercido_acumulado'] / subtotal['Modificado_anual'] if subtotal['Modificado_anual'] != 0 else 0
        subtotal['Pct_avance_periodo'] = subtotal['Ejercido_acumulado'] / subtotal['Modificado_periodo'] if subtotal['Modificado_periodo'] != 0 else 0
        return subtotal
    
    subtotal_sc = calcular_subtotal(config['sector_central'])
    subtotal_of = calcular_subtotal(config['oficinas'])
    subtotal_od = calcular_subtotal(config['organos_desconcentrados'])
    subtotal_ep = calcular_subtotal(config['entidades_paraestatales'])
    
    # Total general
    total_general = {
        'Original': subtotal_sc['Original'] + subtotal_of['Original'] + subtotal_od['Original'] + subtotal_ep['Original'],
        'Modificado_anual': subtotal_sc['Modificado_anual'] + subtotal_of['Modificado_anual'] + subtotal_od['Modificado_anual'] + subtotal_ep['Modificado_anual'],
        'Modificado_periodo': subtotal_sc['Modificado_periodo'] + subtotal_of['Modificado_periodo'] + subtotal_od['Modificado_periodo'] + subtotal_ep['Modificado_periodo'],
        'Ejercido_acumulado': subtotal_sc['Ejercido_acumulado'] + subtotal_of['Ejercido_acumulado'] + subtotal_od['Ejercido_acumulado'] + subtotal_ep['Ejercido_acumulado'],
        'Disponible_anual': subtotal_sc['Disponible_anual'] + subtotal_of['Disponible_anual'] + subtotal_od['Disponible_anual'] + subtotal_ep['Disponible_anual'],
        'Disponible_periodo': subtotal_sc['Disponible_periodo'] + subtotal_of['Disponible_periodo'] + subtotal_od['Disponible_periodo'] + subtotal_ep['Disponible_periodo'],
    }
    total_general['Pct_avance_anual'] = total_general['Ejercido_acumulado'] / total_general['Modificado_anual'] if total_general['Modificado_anual'] != 0 else 0
    total_general['Pct_avance_periodo'] = total_general['Ejercido_acumulado'] / total_general['Modificado_periodo'] if total_general['Modificado_periodo'] != 0 else 0
    
    # Congelados
    df_para_congelados = df_para_congelados[df_para_congelados['Nueva UR'].astype(str).isin(urs_validas)]
    df_para_congelados = df_para_congelados[~df_para_congelados['Partida'].isin([39801, 39810])]
    df_para_congelados = df_para_congelados[df_para_congelados['CAPITULO'] != 1]
    
    congelado_anual = calcular_congelado_anual(df_para_congelados)
    congelado_periodo = calcular_congelado_periodo(df_para_congelados, mes_archivo)
    
    return {
        'resumen': resumen,
        'subtotales': {
            'sector_central': subtotal_sc,
            'oficinas': subtotal_of,
            'organos_desconcentrados': subtotal_od,
            'entidades_paraestatales': subtotal_ep,
        },
        'congelados': {
            'anual': congelado_anual,
            'periodo': congelado_periodo,
            'texto_anual': numero_a_letras_mx(congelado_anual),
            'texto_periodo': numero_a_letras_mx(congelado_periodo),
        },
        'totales': total_general,
        'metadata': {
            'fecha_archivo': fecha_archivo,
            'mes': mes_archivo,
            'año': año_archivo,
            'registros': len(df),
            'es_cierre': es_cierre_año_anterior,
            'config': config,
        },
        'df_procesado': df,
    }
