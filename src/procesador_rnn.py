import pandas as pd

# Leer el archivo Excel
df = pd.read_excel('C:\Users\Fiorella Victoria\PycharmProjects\PythonProject12\ESTIM_papa_2005-2025 (1).xls')  # Cambia 'tu_archivo.xlsx' por el nombre de tu archivo

# Filtrar registros donde la primera columna es igual a "Turrialba"
# Asumiendo que la primera columna se llama como aparece en tu Excel
primera_columna = df.columns[0]  # Obtiene el nombre de la primera columna
df_turrialba = df[df[primera_columna] == 'Turrialba'].copy()

# Crear el DataFrame resultado
resultado = pd.DataFrame()

# Lista de meses
meses = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
         'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']

# Procesar cada fila de Turrialba
for index, fila in df_turrialba.iterrows():
    # Obtener los valores de la fila (excluyendo la primera columna)
    valores = fila.iloc[1:].dropna().tolist()  # Excluye la primera columna y valores nulos

    # Crear un diccionario para esta fila
    fila_resultado = {primera_columna: 'Turrialba'}

    # Asignar valores alternadamente (producción y área)
    for i, mes in enumerate(meses):
        if i * 2 < len(valores):  # Producción
            fila_resultado[f'{mes}-produccion'] = valores[i * 2]
        if i * 2 + 1 < len(valores):  # Área
            fila_resultado[f'{mes}-area'] = valores[i * 2 + 1]

    # Agregar la fila al resultado
    resultado = pd.concat([resultado, pd.DataFrame([fila_resultado])], ignore_index=True)

# Mostrar el resultado
print("Datos procesados de Turrialba:")
print(resultado)

# Guardar el resultado en un nuevo archivo Excel
resultado.to_excel('turrialba_procesado.xlsx', index=False)
print("\nArchivo guardado como 'turrialba_procesado.xlsx'")

# Opcional: Mostrar solo las primeras columnas para verificar
print("\nPrimeras columnas del resultado:")
print(resultado.iloc[:, :6])  # Muestra las primeras 6 columnas