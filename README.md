# Challenge-Data-Engineer

# Parte 1

## Configuración de ambiente

- en redshift se creará una tabla de control con la siguiente estructura: processed_files(file_name, file_hash, processed_at). 
  - Esta tabla permitirá identificar qué archivos fueron procesados
  - Archivos fueron corregidos
  - Evitar reprocesamientos innecesarios.

## Estructura del pipeline:

- Programación: el DAG de airflow se implementará para ejecutar una vez al día antes del inicio de la jornada laboral. 
  - Programado, por ejemplo, diario a las 6 am. 
- El pipeline procesará archivos con una ventana de 7 días (current_date – 7 días, current_date), 
  - Esto permite capturar archivos con retraso
  - Incorporar correcciones históricas
  - Independencia del momento de llegada del archivo.
- Extracción:  
  - Lectura de archivos desde S3
  - Identificación de archivos por fecha (metadata o convención de nombre si el contexto lo permite)
  - Registro de archivos procesados en processed_files.
  - Operación COPY directa de S3 a tabla staging en Redshift.
- Transformación: en esta etapa 
  - Se validarán y definirán el tipo de los campos
  - Se estandarizarán los campos que apliquen (status)
  - Se eliminarán los duplicados utilizando window functions para priorizar el registro más nuevo (basado en el campo updated_at)
- Carga: 
  - Posteriormente un proceso de merge hacia la tabla final (upsert basado en order_id)
  - Se configurará la persistencia de métricas de carga y validación.

## Tareas del DAG de airflow:

- Listado de objetos en S3, dentro de la ventana de fechas
- Filtrado de archivos no procesados o modificados (comparando con tabla processed_files)
- Ingesta de datos
- Validaciones
- Carga a staging
- Merge a tabla final
- Monitoreo y alertas

## Estrategia de carga

- Incremental
  - Debido al crecimiento de volumen, sería costoso e innecesario reprocesar y cargar el histórico de la data
  - Con estrategia de merge se actualizan registros necesarios
- Se utilizará tabla intermedia staging para:
  - Validación de datos
  - Deduplicación
- Implementación de estrategia merge
  - Si existe se actualiza
  - Si no existe se inserta

## Calidad y Consistencia

- Idempotencia
  - Uso de operaciones merge
  - Order_id como clave principal
  - Control de archivos procesados
- Manejo de duplicados
  - Eliminación de duplicados dando prioridad al registro más reciente

## Validaciones y monitoreo

- Validaciones de datos
  - Campos obligatorios no nulos
  - Validación de rangos
  - Consistencia de timestamps (updated_at >= created_at)
  - Validación de valores permitidos (currency, status)
- Validaciones de calidad
  - Comparación de volumen vs ejecuciones anteriores
  - Detección de anomalías en datos
- Monitoreo y alertas, alertas ante:
  - Falta de datos en la ventana esperada
  - Fallos en ejecución
  - Desviaciones significativas en volumen
  - Registro de métricas de ejecución para análisis posterior

# Parte 2

- Si un archivo no llega un día: No lo trataría necesariamente como fallo técnico del pipeline, sino como evento operativo esperado. El DAG terminaría en estado controlado, registraría no_data_found y enviaría alerta si se supera el SLA esperado. Si el archivo llega al día siguiente (o en los días siguientes) una ejecución posterior lo procesará y tendrá el mismo comportamiento de merge a la tabla final.

- Si un archivo llega 2 veces con contenido diferente: el pipeline al inicio calculará el hash del archivo (huella digital) y consultará  la tabla processed_files para validar si el archivo ya fue procesado, en este caso lo encontrará pero al validar las hash_keys se tomará como una corrección y se reprocesará, utilizando la estrategia de merge para incluir las correcciones.

- Si un archivo llega con una columna adicional: Ante columnas nuevas, el pipeline detectaría drift de esquema y generaría una alerta. Si el campo no es requerido para la tabla final, podría almacenarse temporalmente como metadata o en una columna extra_fields tipo SUPER mientras se evalúa su incorporación formal al modelo.

- Si el volumen crece 10x: Para esto se propuso en el pipeline la extracción de datos mediante operación COPY, eficiente desde S3 a Redshift. También en la tabla final de Redshift se creará una Sort Key y Dist Key que permitirá hacer un merge más eficiente.

- Si el dashboard muestra números distintos a los del sistema transaccional: inicialmente sería identificar en segmento de datos que presenta diferencias (rango de fechas, moneda específica, estado), cual es la magnitud de la diferencia y qué métrica la presenta. Posteriormente una comparación por capas (fuente vs staging, staging vs tabla final, tabla final vs dashboard), que permitiría identificar en qué etapa se presenta la alteración de la data. Con esto se pueden encontrar las causas de la diferencia, que pueden ser por duplicados, diferencia en lógica de negocio dashboard vs sistema transaccional, datos tardíos como se menciona en el enunciado, etc. Finalmente dependiendo de los hallazgos anteriores, se procedería con el ajuste acorde (queries, merge o upsert, entre otras posibilidades)

# Parte 3

```SQL
CREATE TABLE fact_sales (
    order_id      VARCHAR(100) PRIMARY KEY,
    user_id       VARCHAR(100) NOT NULL,
    amount        DECIMAL(18,2) NOT NULL,
    currency      VARCHAR(10) NOT NULL,
    status        VARCHAR(50) NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    source_file   VARCHAR(500) NOT NULL,
    loaded_at     TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(order_id)
SORTKEY(created_at);
```

- Como clave primaria y clave lógica de negocio se escoge order_id, asumiendo un valor único por cada registro, que permitirá estrategia de upsert y eliminación de duplicados
- Como sort key se eligió el campo created_at, ya que sería el principal filtro y dimensión de muchas queries analíticas. Además, el pipeline de ingesta reprocesa por ventanas de tiempo, esta sort key resultaría en menos operaciones de lectura, o sea, más eficiencia.
- Finalmente, al estar almacenados en un ambiente distribuido durante las operaciones de merge, sería más eficiente que la data fuera distribuida de forma ordenada en los diferentes nodos de datos. Es por esto que la dist key es el campo order_id.

## Identificaicón de duplicados

```SQL
SELECT
    order_id,
    COUNT(*) AS records_count
FROM fact_sales
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY records_count DESC;
```

## Total ventas y número de órdenes por día y moneda

```SQL
SELECT
    DATE_TRUNC('day', created_at) AS sales_date,
    currency,
    SUM(amount) AS total_sales,
    COUNT(*) AS total_orders
FROM fact_sales
WHERE status = 'completed'
GROUP BY 1, 2
ORDER BY 1, 2;
```

# Parte 4

## Trade offs asumidos:

- Consistencia – Costo computacional

Para esta propuesta, siempre se procesa una ventana de datos de 7 días basado en la fecha de creación del objeto en S3, lo que aumenta el costo computacional, pero mejora y asegura la consistencia de datos.

- Incremental – Complejidad

En esta solución de propuso una estrategia de carga incremental,  que reduce el volumen de datos procesado cada vez, pero aumenta la complejidad en manejo de duplicados y actualizaciones.

- Staging persistente – Almacenamiento

Mantener una tabla staging con metadata (processed_files) implica mayor uso de almacenamiento, pero facilita auditoría, debugging y reprocesamientos controladas.

- Esquema controlado – Flexibilidad

El diseño propuesto sacrifica la evolución automática del la tabla final, pero brinda más estabilidad y control sobre los servicios de datos.

## Dudas del diseño

El aspecto más vulnerable del diseño es lo sensible que podría ser ante fallas de correcciones históricas por:

- La confiabilidad del campo updated_at como fuente de verdad.
- El cambio de esquema entre correcciones de un mismo archivo/registro.

Estos elementos afectan directamente componentes clave del pipeline, como la lógica de manejo de duplicados, la estrategia de MERGE y la forma en que se maneja la consistencia de los datos. Esto podría llevar más adelante a la necesidad de implementar un enfoque más robusto de manejo de históricos.

## Refuerzos cierre contable

- Automatización validación contra sistema transaccional. Validación y alertas sobre validación de totales diarios, comparación por order_id, desviaciones de datos.
- Reforzar trazabilidad y auditoría: con versionado de datos, metadata con la tabla processed_files y capacidad de reconstruir snapshots de data.
- Implementación de control de accesos y gobierno de datos formal para todo el dataset.
