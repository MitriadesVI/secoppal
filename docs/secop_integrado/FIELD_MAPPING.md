# FIELD_MAPPING — SECOP Integrado (rpmr-utcd) → NormalizedContractRecord

## Modelo Normalizado: `NormalizedContractRecord`

Este modelo representa un contrato firmado en cualquier plataforma (SECOP I o II).
Los campos marcados como `OPTIONAL` pueden ser `None` si el registro origen no
los contiene (ej: contratista "NO DEFINIDO" en procesos precontractuales).

```python
from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class NormalizedContractRecord:
    """Registro normalizado de contrato desde SECOP Integrado (rpmr-utcd)."""

    # ── Origen ──
    source_dataset: str          # "rpmr-utcd"
    source_platform: str         # "SECOP I" | "SECOP II"

    # ── Identificadores ──
    contract_id: str             # numero_del_contrato
    process_reference: str       # numero_de_proceso

    # ── Entidad ──
    entity_name: str             # nombre_de_la_entidad
    entity_nit: str              # nit_de_la_entidad
    department: str              # departamento_entidad
    city: str                    # municipio_entidad

    # ── Objeto y Modalidad ──
    object_description: str      # objeto_del_proceso
    modality: str                # modalidad_de_contrataci_n
    contract_type: str           # tipo_de_contrato

    # ── Estado ──
    process_status: str          # estado_del_proceso

    # ── Valor ──
    contract_value: float        # valor_contrato (0 si no definido)

    # ── Fechas ──
    signature_date: Optional[date]    # fecha_de_firma_del_contrato
    start_date: Optional[date]        # fecha_inicio_ejecuci_n
    end_date: Optional[date]          # fecha_fin_ejecuci_n

    # ── Contratista ──
    contractor_name: Optional[str]        # nom_raz_social_contratista
    contractor_document: Optional[str]    # documento_proveedor
    contractor_doc_type: Optional[str]    # tipo_documento_proveedor

    # ── URL ──
    url: str                     # url_contrato
```

---

## Mapping Campo a Campo

| NormalizedContractRecord | rpmr-utcd | Tipo | Notas |
|--------------------------|-----------|------|-------|
| `source_dataset` | — | str | Constante: `"rpmr-utcd"` |
| `source_platform` | `origen` | str | Mapear: `"SECOPI"` → `"SECOP I"`, `"SECOPII"` → `"SECOP II"` |
| `contract_id` | `numero_del_contrato` | str | ID no nulo incluso en Borrador |
| `process_reference` | `numero_de_proceso` | str | ID del proceso |
| `entity_name` | `nombre_de_la_entidad` | str | Nombre completo |
| `entity_nit` | `nit_de_la_entidad` | str | Puede contener guiones (ej: "890503373-0") |
| `department` | `departamento_entidad` | str | Nombre del departamento |
| `city` | `municipio_entidad` | str | Nombre del municipio |
| `object_description` | `objeto_del_proceso` | str | Descripción detallada |
| `modality` | `modalidad_de_contrataci_n` | str | Texto completo |
| `contract_type` | `tipo_de_contrato` | str | Ej: "Prestación de Servicios" |
| `process_status` | `estado_del_proceso` | str | Valor directo |
| `contract_value` | `valor_contrato` | float | Convertir de string. "0" → 0.0 |
| `signature_date` | `fecha_de_firma_del_contrato` | date\|None | Puede ser null en Borrador/Convocado |
| `start_date` | `fecha_inicio_ejecuci_n` | date\|None | Puede ser null |
| `end_date` | `fecha_fin_ejecuci_n` | date\|None | Puede ser null |
| `contractor_name` | `nom_raz_social_contratista` | str\|None | "NO DEFINIDO" → None |
| `contractor_document` | `documento_proveedor` | str\|None | "NO DEFINIDO" → None |
| `contractor_doc_type` | `tipo_documento_proveedor` | str\|None | "NO DEFINIDO" → None |
| `url` | `url_contrato` | str | URL a contratos.gov.co |

---

## Reglas de Transformación

### 1. Valores centinela → None
```python
_SENTINEL = "NO DEFINIDO"

def _clean_sentinel(value: str | None) -> str | None:
    if value is None or value.strip().upper() == _SENTINEL:
        return None
    return value.strip()
```

### 2. Origen: normalizar espacios
```python
_ORIGIN_MAP = {"SECOPI": "SECOP I", "SECOPII": "SECOP II"}

def _normalize_origin(raw: str) -> str:
    return _ORIGIN_MAP.get(raw.strip(), raw.strip())
```

### 3. Valor: string → float
```python
def _parse_value(raw: str | None) -> float:
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (ValueError, TypeError):
        return 0.0
```

### 4. Fecha: string ISO → date
```python
from datetime import date, datetime

def _parse_date(raw: str | None) -> date | None:
    if raw is None:
        return None
    try:
        # Socrata devuelve "2025-01-15T00:00:00.000"
        return datetime.fromisoformat(raw.replace("T00:00:00.000", "")).date()
    except (ValueError, TypeError):
        return None
```

### 5. Filtro de años inválidos
```python
def _valid_year(d: date | None) -> bool:
    if d is None:
        return True  # Permitir null
    return 2000 <= d.year <= 2030
```

---

## Campos NO Mapeados de rpmr-utcd

| Campo rpmr-utcd | Razón para no mapear |
|-----------------|---------------------|
| `nivel_entidad` | Derivado del nombre de entidad. Baja utilidad analítica inmediata. |
| `codigo_entidad_en_secop` | Redundante con `nit_de_la_entidad` + `nombre_de_la_entidad`. |
| `objeto_a_contratar` | Es el código UNSPSC o "NO DEFINIDO". `objeto_del_proceso` es más informativo. |

---

## Volumen y Performance

- 21.9M registros totales
- ~16.2M con contrato firmado (Celebrado + Liquidado + En ejecución + Cerrado)
- ~5.5M precontractuales (Borrador, Convocado, Activo, etc.)
- Para búsqueda histórica de contratos: filtrar SIEMPRE por estado post-adjudicación.
- Timeout recomendado: 60s mínimo. El dataset es grande y Socrata puede ser lento.
