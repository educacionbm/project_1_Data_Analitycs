"""
DESCRIPCIÓN GENERAL DEL CÓDIGO:
    Este es un srcipt que se usa para cambiar el formato A de un archivo a un 
    formato B. Con lo que se consigue minimizar el tiempo de ingreso de datos
    a un sistema. El cual se realizaba de forma manual, lo que ocasionaba retrasos
    en el trabajo y posibles errores de transcripción humanos.
"""

import pandas as pd
import regex as re

# 1. Carga los datos del archivo 'rtu.txt' y los almacena en un dataframe llamado 'datos_brutos'
datos_brutos = pd.read_table('rtu.txt',sep = " ",names=["A","B","C","D","E","F"])
#print(datos_brutos)


# 2. Identificando el tipo de puntos
#Crea un dataframe llamado 'SPI' que incluye las filas de 'datos_brutos' cuyo valor de la columna 'A' termine en '=1'
SPI = datos_brutos[datos_brutos["A"].str.endswith("=1", na=False)]
#print(SPI)

# Crea un dataframe llamado 'DPI' que incluye las filas de 'datos_brutos' cuyo valor de la columna 'A' termine en '=2'
DPI = datos_brutos[datos_brutos["A"].str.endswith("=2",na=False)]
#print(DPI)

# Crea un dataframe llamado 'MVNV' que incluye las filas de 'datos_brutos' cuyo valor de la columna 'A' termine en '=3'
MVNV = datos_brutos[datos_brutos["A"].str.endswith("=3",na=False)]
#print(MVNV)

# Crea un dataframe llamado 'MVFPV' que incluye las filas de 'datos_brutos' cuyo valor de la columna 'A' termine en '=6'
MVFPV = datos_brutos[datos_brutos["A"].str.endswith("=6", na=False)]
#print(MVFPV)

# Crea un dataframe llamado 'MVSV' que incluye las filas de 'datos_brutos' cuyo valor de la columna 'A' termine en '=10'
MVSV = datos_brutos[datos_brutos["A"].str.endswith("=10",na=False)]
#print(MVSV)


# 3. Cambiar valor de los puntos
#Función que modifica el valor de una columna 'A' de un dataframe según un valor antiguo y uno nuevo especificados
def modify_values(x, old_value, new_value):
    return re.sub(r'{}'.format(old_value), new_value, x)

# Modifica el valor de la columna 'A' del dataframe 'MVNV' reemplazando '=3' por '=6'
MVNV['A'] = MVNV['A'].apply(modify_values, args=('=3', '=6'))

# Modifica el valor de la columna 'A' del dataframe 'MVFPV' reemplazando '=6' por '=7'
MVFPV['A'] = MVFPV['A'].apply(modify_values, args=('=6', '=7'))

# Modifica el valor de la columna 'A' del dataframe 'MVSV' reemplazando '=10' por '=12'
MVSV['A'] = MVSV['A'].apply(modify_values, args=('=10', '=12'))


# 4. Filtrando los datos con valores
# Crear dataframe vacío
df_resultado = pd.DataFrame()

# Añadir filas al de SPI si no está vacío al nuevo dataframe
if SPI.size > 0:
    df_resultado = pd.concat([df_resultado, SPI])

# Añadir filas de DPI si no está vacío al nuevo dataframe
if DPI.size > 0:
    df_resultado = pd.concat([df_resultado, DPI])

# Añadir filas de MVNV si no está vacío al nuevo dataframe
if MVNV.size > 0:
    df_resultado = pd.concat([df_resultado, MVNV])    

# Añadir filas de MVFPV si no está vacío al nuevo dataframe
if MVFPV.size > 0:
    df_resultado = pd.concat([df_resultado, MVFPV])

# Añadir filas de MVSV si no está vacío al nuevo dataframe
if MVSV.size > 0:
    df_resultado = pd.concat([df_resultado, MVSV])
        
    
# 5.Creando archivo nuevo
# Filtrando el dataframe resultante solo con las columnas "A" y "B" y agregando las columnas "C" y "D" con volores en todas sus celdas de 0 y 1 respectivamente
df_resultado = df_resultado[["A", "B"]].assign(C=0, D=1)

# Creando archivo nuevo con el dataframe resultante
df_resultado.to_csv("SCADA.txt", sep=" ", index=False, header=False)