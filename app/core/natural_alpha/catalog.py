"""Human-work vocabulary and host-authored labels; never executable routes."""

ACTION_ASPECTS = {
    "SEARCH": ("PROCESSES", "CONTRACTS", "PROCESSES_AND_CONTRACTS", "OPEN_OPPORTUNITIES"),
    "ANALYZE": (
        "OVERVIEW", "VALUES", "VALUE_STATS", "TOTAL_VALUE", "SUPPLIERS", "MODALITIES",
        "RECENT", "TEMPORAL", "EXECUTION", "TOP_ENTITIES",
    ),
    "FOLLOW": ("SAVE", "LIST", "RUN"),
    "RESULT_ACTION": ("PAGINATE", "EXPORT_XLSX"),
    "LOOKUP": ("SECOP_REFERENCE",),
    "OTHER": ("OTHER",),
}

LABELS = {
    "PROCESSES": "Buscar procesos",
    "CONTRACTS": "Buscar contratos",
    "PROCESSES_AND_CONTRACTS": "Buscar procesos y contratos",
    "OPEN_OPPORTUNITIES": "Buscar oportunidades abiertas",
    "OVERVIEW": "Obtener una visión general",
    "VALUES": "Analizar valores",
    "VALUE_STATS": "Examinar estadísticas de valores",
    "TOTAL_VALUE": "Calcular el valor total",
    "SUPPLIERS": "Explorar proveedores",
    "MODALITIES": "Analizar modalidades de contratación",
    "RECENT": "Revisar la contratación reciente",
    "TEMPORAL": "Analizar la evolución en el tiempo",
    "EXECUTION": "Analizar la ejecución contractual",
    "TOP_ENTITIES": "Comparar entidades contratantes",
    "SAVE": "Guardar un seguimiento",
    "LIST": "Ver los seguimientos guardados",
    "RUN": "Consultar novedades de los seguimientos",
    "PAGINATE": "Ver más resultados",
    "EXPORT_XLSX": "Exportar resultados a Excel",
    "SECOP_REFERENCE": "Consultar una referencia SECOP",
    "OTHER": "Otra solicitud",
}
