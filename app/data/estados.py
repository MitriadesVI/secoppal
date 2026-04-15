"""
Estado mappings for SECOP II datasets.
Values verified from discover_secop.py output — April 2026.

Procesos has TWO state fields:
  - estado_de_apertura_del_proceso: only "Abierto" / "Cerrado"
  - estado_del_procedimiento: "Seleccionado", "Publicado", "Evaluación", etc.

Contratos has ONE state field:
  - estado_contrato: "En ejecución", "Cerrado", "Modificado", etc.
  NOTE: SECOP has inconsistent casing — "terminado" and "cedido" are lowercase!
"""

# ---------------------------------------------------------------------------
# PROCESOS — estado_de_apertura_del_proceso (only 2 values)
# ---------------------------------------------------------------------------
ESTADO_APERTURA_SYNONYMS = {
    "abierto": "Abierto",
    "abierta": "Abierto",
    "abiertos": "Abierto",
    "abiertas": "Abierto",
    "vigente": "Abierto",
    "vigentes": "Abierto",
    "activo": "Abierto",
    "activos": "Abierto",
    "activa": "Abierto",
    "activas": "Abierto",
    "cerrado": "Cerrado",
    "cerrada": "Cerrado",
    "cerrados": "Cerrado",
    "cerradas": "Cerrado",
}

# PROCESOS — estado_del_procedimiento (9 values)
ESTADO_PROCEDIMIENTO_SYNONYMS = {
    "seleccionado": "Seleccionado",
    "adjudicado": "Seleccionado",  # colloquial → SECOP says "Seleccionado"
    "adjudicada": "Seleccionado",
    "adjudicados": "Seleccionado",
    "publicado": "Publicado",
    "publicada": "Publicado",
    "publicados": "Publicado",
    "evaluacion": "Evaluación",
    "en evaluacion": "Evaluación",
    "evaluando": "Evaluación",
    "cancelado": "Cancelado",
    "cancelada": "Cancelado",
    "cancelados": "Cancelado",
    "borrador": "Borrador",
    "aprobado": "Aprobado",
    "aprobada": "Aprobado",
    "en aprobacion": "En aprobación",
    "suspendido": "Suspendido",
    "suspendida": "Suspendido",
}

VALID_ESTADOS_APERTURA = {"Abierto", "Cerrado"}

VALID_ESTADOS_PROCEDIMIENTO = {
    "Seleccionado", "Publicado", "Evaluación", "Cancelado",
    "Borrador", "Abierto", "Aprobado", "En aprobación", "Suspendido",
}

# ---------------------------------------------------------------------------
# CONTRATOS — estado_contrato (12 values)
# NOTE: "terminado", "cedido", "enviado Proveedor" are lowercase in SECOP!
# ---------------------------------------------------------------------------
ESTADO_CONTRATO_SYNONYMS = {
    "en ejecucion": "En ejecución",
    "ejecutando": "En ejecución",
    "ejecutandose": "En ejecución",
    "en curso": "En ejecución",
    "vigente contrato": "En ejecución",
    "cerrado": "Cerrado",
    "cerrada": "Cerrado",
    "cerrados": "Cerrado",
    "modificado": "Modificado",
    "modificada": "Modificado",
    "modificados": "Modificado",
    "terminado": "terminado",  # ← lowercase in SECOP!
    "terminada": "terminado",
    "terminados": "terminado",
    "finalizado": "terminado",
    "finalizada": "terminado",
    "firmado": "Aprobado",  # "firmado" → closest match is Aprobado
    "firmada": "Aprobado",
    "firmados": "Aprobado",
    "aprobado": "Aprobado",
    "aprobada": "Aprobado",
    "cancelado": "Cancelado",
    "cancelada": "Cancelado",
    "cancelados": "Cancelado",
    "borrador": "Borrador",
    "cedido": "cedido",  # ← lowercase in SECOP!
    "cedida": "cedido",
    "suspendido": "Suspendido",
    "suspendida": "Suspendido",
    "prorrogado": "Prorrogado",
    "prorrogada": "Prorrogado",
    "enviado proveedor": "enviado Proveedor",  # ← weird casing in SECOP
    "liquidado": "Cerrado",  # no "Liquidado" state; closest is "Cerrado"
    "liquidada": "Cerrado",
    "liquidados": "Cerrado",
}

VALID_ESTADOS_CONTRATO = {
    "En ejecución", "Cerrado", "Modificado", "terminado",
    "Borrador", "Aprobado", "Cancelado", "enviado Proveedor",
    "cedido", "En aprobación", "Suspendido", "Prorrogado",
}

# ---------------------------------------------------------------------------
# Combined for backward compat (QueryRouter uses dataset-specific ones)
# ---------------------------------------------------------------------------
ESTADO_SYNONYMS = {**ESTADO_APERTURA_SYNONYMS, **ESTADO_CONTRATO_SYNONYMS}
VALID_ESTADOS = VALID_ESTADOS_APERTURA | VALID_ESTADOS_CONTRATO
