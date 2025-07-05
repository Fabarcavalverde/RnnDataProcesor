"""
Pipeline centralizado para el procesamiento completo de datos.
"""

import pandas as pd
import os
import logging
from typing import Optional, Dict, Any
from datetime import datetime

from src.procesador_datos_papa import ProcesadorDatosPapa
from src.procesador_datos_atmosfericos import ProcesadorDatosAtmosfericos
from src.merge_datos_papa_atmosfericos import MergeDatosPapaAtmosfericos


class PipelineProcesamiento:
    """
    Pipeline centralizado para procesar datos de papa y clima.
    """

    def __init__(self,
                 ruta_excel_papa: str,
                 carpeta_datos_atmosfericos: str,
                 carpeta_salida: str = "data/processed",
                 log_level: str = "INFO"):
        """
        Inicializa el pipeline con las rutas necesarias.

        Args:
            ruta_excel_papa (str): Ruta del archivo Excel con datos de papa
            carpeta_datos_atmosfericos (str): Carpeta con archivos CSV de datos atmosféricos
            carpeta_salida (str): Carpeta donde guardar resultados
            log_level (str): Nivel de logging (DEBUG, INFO, WARNING, ERROR)
        """
        self.ruta_excel_papa = ruta_excel_papa
        self.carpeta_datos_atmosfericos = carpeta_datos_atmosfericos
        self.carpeta_salida = carpeta_salida

        # Configurar logging
        self._configurar_logging(log_level)
        self.logger = logging.getLogger(__name__)

        # Crear carpeta de salida si no existe
        os.makedirs(self.carpeta_salida, exist_ok=True)

        self.logger.info("Pipeline inicializado correctamente")

    def _configurar_logging(self, log_level: str) -> None:
        """
        Configura el sistema de logging.

        Args:
            log_level (str): Nivel de logging
        """
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'pipeline_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            ]
        )

    def _validar_archivos_entrada(self) -> None:
        """
        Valida que existan los archivos y carpetas de entrada.

        Raises:
            FileNotFoundError: Si faltan archivos o carpetas
        """
        if not os.path.exists(self.ruta_excel_papa):
            raise FileNotFoundError(f"Archivo Excel no encontrado: {self.ruta_excel_papa}")

        if not os.path.exists(self.carpeta_datos_atmosfericos):
            raise FileNotFoundError(f"Carpeta de datos atmosféricos no encontrada: {self.carpeta_datos_atmosfericos}")

        if not os.path.isdir(self.carpeta_datos_atmosfericos):
            raise NotADirectoryError(f"La ruta no es una carpeta válida: {self.carpeta_datos_atmosfericos}")

        # Verificar que hay archivos CSV en la carpeta
        archivos_csv = [f for f in os.listdir(self.carpeta_datos_atmosfericos) if f.endswith('.csv')]
        if not archivos_csv:
            raise FileNotFoundError(f"No se encontraron archivos CSV en: {self.carpeta_datos_atmosfericos}")

        self.logger.info("Validación de archivos de entrada completada")

    def procesar_datos_papa(self) -> pd.DataFrame:
        """
        Procesa los datos de papa.

        Returns:
            pd.DataFrame: DataFrame procesado
        """
        try:
            self.logger.info("Iniciando procesamiento de datos de papa")

            procesador_papa = ProcesadorDatosPapa(self.ruta_excel_papa)
            df_papa = procesador_papa.procesar_a_formato_largo()

            self.logger.info("Procesamiento de datos de papa completado")
            return df_papa

        except Exception as e:
            self.logger.error(f"Error procesando datos de papa: {e}")
            raise

    def procesar_datos_atmosfericos(self) -> pd.DataFrame:
        """
        Procesa los datos atmosféricos.

        Returns:
            pd.DataFrame: DataFrame procesado
        """
        try:
            self.logger.info("Iniciando procesamiento de datos atmosféricos")

            procesador_atmosferico = ProcesadorDatosAtmosfericos(
                self.carpeta_datos_atmosfericos,
                self.carpeta_salida
            )

            df_clima = procesador_atmosferico.procesar_todos()

            self.logger.info("Procesamiento de datos atmosféricos completado")
            return df_clima

        except Exception as e:
            self.logger.error(f"Error procesando datos atmosféricos: {e}")
            raise

    def fusionar_datos(self, df_papa: pd.DataFrame, df_clima: pd.DataFrame) -> pd.DataFrame:
        """
        Fusiona los datos de papa y clima.

        Args:
            df_papa (pd.DataFrame): DataFrame de datos de papa
            df_clima (pd.DataFrame): DataFrame de datos climáticos

        Returns:
            pd.DataFrame: DataFrame fusionado
        """
        try:
            self.logger.info("Iniciando fusión de datos")

            # Crear archivos temporales para el merge
            import tempfile

            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_clima:
                df_clima.to_csv(tmp_clima.name, index=False, encoding='utf-8')
                ruta_tmp_clima = tmp_clima.name

            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_papa:
                df_papa.to_csv(tmp_papa.name, index=False, encoding='utf-8')
                ruta_tmp_papa = tmp_papa.name

            # Realizar la fusión
            fusionador = MergeDatosPapaAtmosfericos(ruta_tmp_clima, ruta_tmp_papa)
            df_fusionado = fusionador.unir_datasets()

            # Limpiar archivos temporales
            os.unlink(ruta_tmp_clima)
            os.unlink(ruta_tmp_papa)

            self.logger.info("Fusión completada")
            return df_fusionado

        except Exception as e:
            self.logger.error(f"Error fusionando datos: {e}")
            raise

    def generar_reporte_calidad(self, df_final: pd.DataFrame) -> Dict[str, Any]:
        """
        Genera un reporte de calidad de los datos procesados.

        Args:
            df_final (pd.DataFrame): DataFrame final procesado

        Returns:
            Dict[str, Any]: Reporte de calidad
        """
        try:
            reporte = {
                "total_filas": len(df_final),
                "total_columnas": len(df_final.columns),
                "cantones_unicos": df_final['canton'].nunique() if 'canton' in df_final.columns else 0,
                "años_unicos": df_final['anio'].nunique() if 'anio' in df_final.columns else 0,
                "meses_unicos": df_final['mes'].nunique() if 'mes' in df_final.columns else 0,
                "valores_faltantes": df_final.isnull().sum().sum(),
                "porcentaje_completitud": (1 - (
                            df_final.isnull().sum().sum() / (len(df_final) * len(df_final.columns)))) * 100
            }

            self.logger.info(f"Reporte de calidad generado: {reporte}")
            return reporte

        except Exception as e:
            self.logger.error(f"Error generando reporte de calidad: {e}")
            return {}

    def ejecutar_pipeline_completo(self) -> tuple[str, Dict[str, Any]]:
        """
        Ejecuta todo el pipeline de procesamiento.

        Returns:
            tuple: (ruta_archivo_final, reporte_calidad)

        Raises:
            Exception: Si hay errores en cualquier paso del pipeline
        """
        try:
            inicio = datetime.now()
            self.logger.info("=== INICIANDO PIPELINE COMPLETO ===")

            # Validar archivos de entrada
            self._validar_archivos_entrada()

            # Procesar datos de papa
            df_papa = self.procesar_datos_papa()

            # Procesar datos atmosféricos
            df_clima = self.procesar_datos_atmosfericos()

            # Fusionar datos
            df_final = self.fusionar_datos(df_papa, df_clima)

            # Guardar resultado final
            ruta_final = os.path.join(self.carpeta_salida, "rnn_df.csv")
            df_final.to_csv(ruta_final, index=False, encoding='utf-8')

            # Generar reporte de calidad
            reporte = self.generar_reporte_calidad(df_final)

            # Calcular tiempo total
            tiempo_total = datetime.now() - inicio

            self.logger.info(f"=== PIPELINE COMPLETADO EXITOSAMENTE ===")
            self.logger.info(f"Tiempo total: {tiempo_total}")
            self.logger.info(f"Archivo final: {ruta_final}")

            return ruta_final, reporte

        except Exception as e:
            self.logger.error(f"Error en pipeline completo: {e}")
            raise
