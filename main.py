from procesador_rnn import ProcesadorDatosPapa

if __name__ == "__main__":
    # Crear instancia del procesador
    archivo = r"C:\Users\Fiorella Victoria\PycharmProjects\PythonProject12\ESTIM_papa_2005-2025 (1).xls"
    procesador = ProcesadorDatosPapa(archivo)

    # Procesar directamente a formato largo y exportar
    procesador.procesar_y_exportar("datos_transformados.csv")
