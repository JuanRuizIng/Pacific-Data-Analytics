import requests
import pandas as pd
import io
import csv

url = "https://www.datos.gov.co/resource/s54a-sgyg.csv?$query=SELECT%20%60codigoestacion%60,%20%60codigosensor%60,%20%60fechaobservacion%60,%20%60valorobservado%60,%20%60nombreestacion%60,%20%60departamento%60,%20%60municipio%60,%20%60zonahidrografica%60,%20%60latitud%60,%20%60longitud%60,%20%60descripcionsensor%60,%20%60unidadmedida%60%20WHERE%20caseless_one_of(%60departamento%60,%22VALLE%20DEL%20CAUCA%22,%22CHOC%C3%93%22,%22CHOCO%22,%22NARI%C3%91O%22,%22NARINO%22,%22CAUCA%22)%20LIMIT%2035000000%20OFFSET%200&$$app_token=7DJ4ddS576T0c1MrFzJqNLP2u"

response = requests.get(url)

data = io.StringIO(response.text)
dialect = csv.Sniffer().sniff(data.read(1024))
data.seek(0)
df = pd.read_csv(data, delimiter=dialect.delimiter)
print(df.head(-1))
print(len(df))

# Imprime las primeras 3 filas del DataFrame
print(df.head(3))
print(len(df))

datos_vacios = df.isnull().sum(axis=1).sum()
#print(datos_vacios)
if datos_vacios > 0:
    df = df.dropna()

df = df.drop(['codigoestacion', 'codigosensor', 'descripcionsensor', 'unidadmedida'], axis=1)

df['departamento'] = df['departamento'].replace({'NARINO': 'NARIÑO', 'CHOCO': 'CHOCÓ'})

print(df['departamento'].unique())
print(len(df))
print(df.head(3))

# Convierte la columna 'fechaobservacion' a datetime
df['fechaobservacion'] = pd.to_datetime(df['fechaobservacion'])

# Crea nuevas columnas para el año, mes, día y hora
df['año'] = df['fechaobservacion'].dt.year
df['mes'] = df['fechaobservacion'].dt.month
df['día'] = df['fechaobservacion'].dt.day
df['hora'] = df['fechaobservacion'].dt.hour

# Mapear los meses a su nombre
meses_map = {
    1: 'Enero',
    2: 'Febrero',
    3: 'Marzo',
    4: 'Abril',
    5: 'Mayo',
    6: 'Junio',
    7: 'Julio',
    8: 'Agosto',
    9: 'Septiembre',
    10: 'Octubre',
    11: 'Noviembre',
    12: 'Diciembre'
}

df['mesOrden'] = df['mes']
df['mes'] = df['mes'].map(meses_map)

columnas = list(df.columns)

indice_mes = columnas.index('mes')

# Se remueve "mesOrden" para evitar duplicados si ya existe
columnas.remove('mesOrden')
columnas.insert(indice_mes + 1, 'mesOrden')

# Reordena el DataFrame según la nueva lista de columnas
df = df[columnas]

print(df["día"])

print(f"El número de filas es {df.shape[0]}")

df = df.reset_index(drop=True)

df = df.drop(['fechaobservacion'], axis=1)

print(df.head(5))

# Guarda el DataFrame como un archivo CSV
df.to_csv('precipitacionesAPI.csv', index=False)