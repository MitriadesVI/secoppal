# FIELD_MAPPING — SECOP I Procesos (f789-7hwg) → NormalizedHistoricalProcessRecord

## Modelo Normalizado: `NormalizedHistoricalProcessRecord`

Este modelo representa un proceso de compra pública histórica de SECOP I,
independientemente de si llegó a contrato o no. Captura tanto la fase
precontractual como la adjudicación (si existe).

```python
from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class NormalizedHistoricalProcessRecord:
    """Registro normalizado de proceso histórico SECOP I (f789-7hwg)."""

    # ── Origen ──
    source_dataset: str          # "f789-7hwg"

    # ── Identificadores ──
    process_id: str              # uid (ID único de la relación)
    process_reference: str       # numero_de_constancia o numero_de_proceso

    # ── Entidad ──
    entity_name: str             # nombre_entidad
    entity_nit: str              # nit_de_la_entidad
    department: str              # departamento_entidad
    city: str                    # municipio_entidad

    # ── Objeto y Modalidad ──
    object_description: str      # detalle_del_objeto_a_contratar (preferido) o objeto_a_contratar
    unspsc_group: Optional[str]  # nombre_grupo
    unspsc_family: Optional[str] # nombre_familia
    unspsc_class: Optional[str]  # nombre_clase
    modality: str                # modalidad_de_contratacion
    contract_type: Optional[str] # tipo_de_contrato

    # ── Estado ──
    process_status: str          # estado_del_proceso

    # ── Valores ──
    estimated_value: float       # cuantia_proceso
    contract_value: Optional[float]  # cuantia_contrato (None si no hay contrato)
    contract_value_with_additions: Optional[float]  # valor_contrato_con_adiciones

    # ── Fechas ──
    publication_date: Optional[date]   # fecha_de_cargue_en_el_secop
    signature_date: Optional[date]     # fecha_de_firma_del_contrato
    start_date: Optional[date]         # fecha_ini_ejec_contrato
    end_date: Optional[date]           # fecha_fin_ejec_contrato
    liquidation_date: Optional[date]   # fecha_liquidacion

    # ── Contratista (si existe) ──
    contractor_name: Optional[str]        # nom_razon_social_contratista
    contractor_document: Optional[str]    # identificacion_del_contratista
    contractor_doc_type: Optional[str]    # tipo_identifi_del_contratista

    # ── URL ──
    url: Optional[str]           # ruta_proceso_en_secop_i

    # ── Extras analíticos ──
    has_contract: bool           # derivado: signature_date IS NOT NULL
    is_postconflict: bool        # es_postconflicto == 1
```

---

## Mapping Campo a Campo

| NormalizedHistoricalProcessRecord | f789-7hwg | Tipo | Notas |
|----------------------------------|-----------|------|-------|
| `source_dataset` | — | str | Constante: `"f789-7hwg"` |
| `process_id` | `uid` | str | ID único de la relación proceso-adjudicación |
| `process_reference` | `numero_de_constancia` | str | Fallback: `numero_de_proceso` |
| `entity_name` | `nombre_entidad` | str | Formato "DEPTO - ENTIDAD" |
| `entity_nit` | `nit_de_la_entidad` | str | NIT |
| `department` | `departamento_entidad` | str | Departamento |
| `city` | `municipio_entidad` | str | Municipio |
| `object_description` | `detalle_del_objeto_a_contratar` | str | Fallback: `objeto_a_contratar` |
| `unspsc_group` | `nombre_grupo` | str\|None | Categoría UNSPSC nivel 1 |
| `unspsc_family` | `nombre_familia` | str\|None | Categoría UNSPSC nivel 2 |
| `unspsc_class` | `nombre_clase` | str\|None | Categoría UNSPSC nivel 3 |
| `modality` | `modalidad_de_contratacion` | str | Texto completo |
| `contract_type` | `tipo_de_contrato` | str\|None | Puede ser null |
| `process_status` | `estado_del_proceso` | str | Valor directo |
| `estimated_value` | `cuantia_proceso` | float | Valor de lanzamiento |
| `contract_value` | `cuantia_contrato` | float\|None | None si no hay contrato |
| `contract_value_with_additions` | `valor_contrato_con_adiciones` | float\|None | None si no hay |
| `publication_date` | `fecha_de_cargue_en_el_secop` | date\|None | Fecha de publicación en SECOP I |
| `signature_date` | `fecha_de_firma_del_contrato` | date\|None | None si `anno_firma_contrato = "Sin Firma"` |
| `start_date` | `fecha_ini_ejec_contrato` | date\|None | Inicio de ejecución |
| `end_date` | `fecha_fin_ejec_contrato` | date\|None | Fin de ejecución |
| `liquidation_date` | `fecha_liquidacion` | date\|None | Fecha de liquidación |
| `contractor_name` | `nom_razon_social_contratista` | str\|None | Puede ser null |
| `contractor_document` | `identificacion_del_contratista` | str\|None | Puede ser null |
| `contractor_doc_type` | `tipo_identifi_del_contratista` | str\|None | Puede ser null |
| `url` | `ruta_proceso_en_secop_i` | str\|None | URL directa a contratos.gov.co |
| `has_contract` | — | bool | Derivado: `signature_date is not None` |
| `is_postconflict` | `es_postconflicto` | bool | `== 1` |

---

## Reglas de Transformación

### 1. Valores numéricos → float
```python
def _parse_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None

def _parse_float_zero(raw: str | None) -> float:
    val = _parse_float(raw)
    return val if val is not None else 0.0
```

### 2. Fecha → date
```python
from datetime import date, datetime

def _parse_date(raw: str | None) -> date | None:
    if raw is None:
        return None
    try:
        # Socrata usa "2018-03-15T00:00:00.000"
        return datetime.fromisoformat(raw.replace("T00:00:00.000", "")).date()
    except (ValueError, TypeError):
        return None
```

### 3. Booleano desde número
```python
def _parse_bool_int(raw: str | None) -> bool:
    if raw is None:
        return False
    try:
        return int(raw) == 1
    except (ValueError, TypeError):
        return False
```

### 4. Object description: preferir detalle sobre código
```python
def _pick_object(row: dict) -> str:
    detail = (row.get("detalle_del_objeto_a_contratar") or "").strip()
    if detail:
        return detail
    return (row.get("objeto_a_contratar") or "").strip()
```

### 5. Process reference: preferir constancia sobre número
```python
def _pick_reference(row: dict) -> str:
    constancia = (row.get("numero_de_constancia") or "").strip()
    if constancia:
        return constancia
    return (row.get("numero_de_proceso") or "").strip()
```

---

## Campos NO Mapeados de f789-7hwg (selección)

| Campo f789-7hwg | Razón para no mapear |
|-----------------|---------------------|
| `nivel_entidad`, `orden_entidad` | Derivados de `nombre_entidad` |
| `c_digo_de_la_entidad` | Redundante con NIT |
| `id_modalidad`, `id_regimen_de_contratacion`, `id_objeto_a_contratar` | IDs numéricos internos de SECOP I |
| `causal_de_otras_formas_de` | Campo muy específico de contratación directa |
| `municipio_de_obtencion`, `municipio_de_entrega`, `municipios_ejecucion` | Múltiples municipios, difícil normalizar |
| `plazo_de_ejec_del_contrato`, `rango_de_ejec_del_contrato` | Información derivada de fechas |
| `tiempo_adiciones_en_dias`, `tiempo_adiciones_en_meses` | Detalle de adiciones |
| `compromiso_presupuestal` | Código interno |
| `proponentes_seleccionados`, `calificacion_definitiva` | Listas de texto |
| `id_sub_unidad_ejecutora`, `nombre_sub_unidad_ejecutora` | Detalle organizacional |
| `posicion_rubro`, `nombre_rubro`, `valor_rubro` | Detalle presupuestal |
| `sexo_replegal` | No relevante para búsqueda |
| `pilar_acuerdo_paz`, `punto_acuerdo_paz` | Detalle postconflicto (ya capturado como bool) |
| `tipo_doc_representante_legal`, `identific_representante_legal`, `nombre_del_represen_legal` | Datos personales |
| `cumpledecreto248`, `incluyebienesdecreto248`, `cumple_sentencia_t302` | Marcadores específicos |
| `es_mipyme`, `tama_o_mipyme` | De la entidad, no del proceso |
| `codigo_bpin`, `destino_gasto` | Detalle presupuestal |
| `pliegos_tipo`, `sector_pliegos_tipo` | Metadata de pliegos |
| `marcacion_adiciones` | Derivado de `valor_total_de_adiciones > 0` |

---

## Volumen y Performance

- 6.37M registros totales
- 5.83M con contrato firmado (91.5%)
- 540K sin contrato (8.5%) — valor analítico alto para trazabilidad
- 79 columnas — solo mapear las necesarias para reducir payload
- Timeout recomendado: 30s (dataset más pequeño y eficiente que rpmr-utcd)
