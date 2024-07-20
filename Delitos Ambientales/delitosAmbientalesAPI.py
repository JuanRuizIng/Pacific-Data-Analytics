import requests
import pandas as pd
import io
import csv
from datetime import datetime

url = 'https://www.datos.gov.co/resource/9zck-qfvc.csv?$query=SELECT%0A%20%20%60fecha_hecho%60%2C%0A%20%20%60cod_depto%60%2C%0A%20%20%60departamento%60%2C%0A%20%20%60cod_muni%60%2C%0A%20%20%60municipio%60%2C%0A%20%20%60descripcion_conducta%60%2C%0A%20%20%60zona%60%2C%0A%20%20%60cantidad%60%0AWHERE%0A%20%20caseless_one_of(%0A%20%20%20%20%60departamento%60%2C%0A%20%20%20%20%22VALLE%20DEL%20CAUCA%22%2C%0A%20%20%20%20%22CAUCA%22%2C%0A%20%20%20%20%22CHOCO%22%2C%0A%20%20%20%20%22NARI%C3%91O%22%2C%0A%20%20%20%20%22CHOC%C3%93%22%2C%0A%20%20%20%20%22NARINO%22%0A%20%20)%20LIMIT%2035000000%20OFFSET%200'

response = requests.get(url)

data = io.StringIO(response.text)
dialect = csv.Sniffer().sniff(data.read(1024))
data.seek(0)
df = pd.read_csv(data, delimiter=dialect.delimiter)
#print(df.head(-1))
#print(len(df))

# Imprime las primeras 3 filas del DataFrame
#print(df.head(3))
#print(len(df))

datos_vacios = df.isnull().sum(axis=1).sum()
#print(datos_vacios)
if datos_vacios > 0:
    df = df.dropna()

df = df.drop(['cod_depto', 'cod_muni'], axis=1)

df['departamento'] = df['departamento'].replace({'NARINO': 'NARIÑO', 'CHOCO': 'CHOCÓ'})

#print(df['zona'].unique())
#print(len(df))
#print(df.head(3))

#print(df['fecha_hecho'].head(5))

# Convierte la columna 'fecha_hecho' a date
df['fecha_hecho'] = df['fecha_hecho'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f").date())

# Crea nuevas columnas para el año, mes y día
df['año'] = df['fecha_hecho'].apply(lambda x: x.year)
df['mes'] = df['fecha_hecho'].apply(lambda x: x.month)
df['día'] = df['fecha_hecho'].apply(lambda x: x.day)

df = df.reset_index(drop=True)

#print(df['año'].unique())

df = df.drop(['fecha_hecho'], axis=1)

#print(df.head(5))

# Elimina las filas con valores nulos
df = df.dropna()

# Elimina los espacios en blanco al inicio y al final de las cadenas
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Convierte la columna 'año' a int
df['año'] = df['año'].astype(int)

print("-------------------------")
#print(df['año'].unique())

# Elimina las filas donde el año es menor a 1900
df = df[df['año'] >= 1900]

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

# Guarda el DataFrame como un archivo CSV
df.to_csv('delitosAmbientalesAPI.csv', index=False)