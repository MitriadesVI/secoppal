"""Fixtures mínimos para Torture Matrix QA-001.

Cubren:
- Contratos con estados reales (En ejecución, Modificado, Prorrogado, Cerrado, terminado, Suspendido, cedido)
- Entidades clave del gazetteer
- Datos suficientes para probar grounding, paginación y degradación sin red.
"""

from __future__ import annotations

import datetime as dt

NOW = dt.datetime(2026, 5, 16)

# ── Contratos base ──────────────────────────────────────────────────────────
CONTRATOS_BASE = [
    {
        "id": "C-001",
        "entidad": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
        "departamento": "ATLANTICO",
        "ciudad": "Barranquilla",
        "objeto_del_contrato": "Mantenimiento preventivo de equipos médicos para atención de adulto mayor",
        "estado_contrato": "En ejecución",
        "valor_del_contrato": 1250000000,
        "fecha_de_firma": "2025-03-12",
        "fecha_de_publicacion_del": "2025-02-28",
    },
    {
        "id": "C-002",
        "entidad": "GOBERNACION DEL ATLANTICO",
        "departamento": "ATLANTICO",
        "ciudad": "Barranquilla",
        "objeto_del_contrato": "Construcción de centros de vida para primera infancia",
        "estado_contrato": "Modificado",
        "valor_del_contrato": 875000000,
        "fecha_de_firma": "2025-01-15",
        "fecha_de_publicacion_del": "2024-12-10",
    },
    {
        "id": "C-003",
        "entidad": "ICBF",
        "departamento": "CUNDINAMARCA",
        "ciudad": "Bogota",
        "objeto_del_contrato": "Servicios de alimentación escolar - PAE",
        "estado_contrato": "En ejecución",
        "valor_del_contrato": 2450000000,
        "fecha_de_firma": "2025-04-01",
        "fecha_de_publicacion_del": "2025-03-20",
    },
    {
        "id": "C-004",
        "entidad": "SENA",
        "departamento": "ANTIOQUIA",
        "ciudad": "Medellin",
        "objeto_del_contrato": "Formación en competencias digitales para jóvenes",
        "estado_contrato": "Prorrogado",
        "valor_del_contrato": 680000000,
        "fecha_de_firma": "2024-11-20",
        "fecha_de_publicacion_del": "2024-10-05",
    },
    {
        "id": "C-005",
        "entidad": "FUNDACARIBE",
        "departamento": "ATLANTICO",
        "ciudad": "Barranquilla",
        "objeto_del_contrato": "Atención integral a población vulnerable - adulto mayor",
        "estado_contrato": "Cerrado",
        "valor_del_contrato": 320000000,
        "fecha_de_firma": "2024-06-30",
        "fecha_de_publicacion_del": "2024-05-12",
    },
    {
        "id": "C-006",
        "entidad": "FUNDACION 2030",
        "departamento": "CUNDINAMARCA",
        "ciudad": "Bogota",
        "objeto_del_contrato": "Fortalecimiento de capacidades para veedurías ciudadanas",
        "estado_contrato": "terminado",
        "valor_del_contrato": 150000000,
        "fecha_de_firma": "2024-09-10",
        "fecha_de_publicacion_del": "2024-08-01",
    },
    {
        "id": "C-007",
        "entidad": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
        "departamento": "ATLANTICO",
        "ciudad": "Barranquilla",
        "objeto_del_contrato": "Mantenimiento de infraestructura portuaria",
        "estado_contrato": "Suspendido",
        "valor_del_contrato": 940000000,
        "fecha_de_firma": "2025-02-05",
        "fecha_de_publicacion_del": "2025-01-18",
    },
    {
        "id": "C-008",
        "entidad": "GOBERNACION DEL ATLANTICO",
        "departamento": "ATLANTICO",
        "ciudad": "Barranquilla",
        "objeto_del_contrato": "Cesión de derechos de contrato de salud",
        "estado_contrato": "cedido",
        "valor_del_contrato": 410000000,
        "fecha_de_firma": "2024-07-22",
        "fecha_de_publicacion_del": "2024-06-15",
    },
]

# ── Procesos (para casos de dataset mixto) ─────────────────────────────────
PROCESOS_BASE = [
    {
        "id": "P-001",
        "entidad": "ALCALDIA DE BOGOTA",
        "departamento": "CUNDINAMARCA",
        "ciudad": "Bogota",
        "objeto_del_contrato": "Adquisición de vehículos para transporte público",
        "estado_de_apertura": "Abierto",
        "valor_estimado": 3200000000,
        "fecha_de_publicacion_del": "2026-05-10",
    },
]

# ── Entidades resueltas (para gazetteer tests) ─────────────────────────────
ENTIDADES_RESUELTAS = {
    "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
    "GOBERNACION DEL ATLANTICO": "GOBERNACION DEL ATLANTICO",
    "ICBF": "ICBF",
    "SENA": "SENA",
    "FUNDACARIBE": "FUNDACARIBE",
    "FUNDACION 2030": "FUNDACION 2030",
}

# ── Helper para mock SecopClient ───────────────────────────────────────────
def make_mock_client(rows: list[dict] | None = None):
    """Devuelve un SecopClient mockeado que no toca red."""
    class _Mock:
        def __init__(self):
            self._rows = rows or CONTRATOS_BASE + PROCESOS_BASE
            self.call_count = 0

        def query(self, dataset_id: str, soql: str):
            self.call_count += 1
            # Filtro muy básico para simular (solo para tests de tortura)
            if "estado_contrato" in soql:
                # Simula filtro de estado
                filtered = [r for r in self._rows if r.get("estado_contrato")]
                return filtered[:10]
            return self._rows[:10]

        def count(self, dataset_id: str, soql: str):
            self.call_count += 1
            return len(self.query(dataset_id, soql))

    return _Mock()