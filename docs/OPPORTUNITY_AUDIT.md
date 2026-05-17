# OPP-001 — Opportunity Hunting Audit

**Fecha:** 2026-05-17  
**Estado:** OPP-002 implementado (policy mínima opportunity_search). Fase 1-4 completadas.  
**Objetivo:** Auditar qué tan bien SECOPPAL encuentra oportunidades SECOP (procesos activos o accionables, no contratos firmados).  
**Alcance:** Solo documentación y tests con mocks. Sin implementación de Radar, notificaciones, WhatsApp/Telegram, H10, AQ-001A, top_entities, ni uso de Licitaciones.info como fuente.

---

## 1. Definición de oportunidad

- **oportunidad activa**: Proceso en estado Publicado, Abierto o Convocatoria con fecha de apertura/fin de presentación vigente o próxima. Es accionable para proponentes.
- **alerta temprana**: Borrador o proceso recién publicado (últimos 7-15 días) que aún no tiene estado final. Útil para seguimiento proactivo.
- **oportunidad vencida**: Proceso cuya fecha de cierre ya pasó pero aún aparece en dataset procesos (puede requerir verificación manual).
- **excluido**: Cualquier proceso que ya pasó a adjudicación, contrato firmado, cancelado o terminado. No es oportunidad.

---

## 2. Estados incluidos

- Publicado
- Abierto
- Convocatoria
- Borrador (solo como alerta temprana si la fecha de publicación es reciente)

---

## 3. Estados excluidos o baja prioridad

- Cancelado
- Adjudicado
- Terminado
- Cerrado
- Contrato firmado (debe ir a dataset contratos)

---

## 4. Reglas

- “oportunidades”, “convocatorias”, “licitaciones abiertas” → dataset=procesos + estado_family=oferta_abierta.
- “contratos firmados”, “contratos adjudicados” → dataset=contratos.
- Timeout ≠ cero resultados. Nunca mostrar “Resultados: 0” cuando el error fue timeout.
- Oportunidades sin fecha/lugar explícito deben acotarse por frescura (ventana temporal controlada de 30-60 días) o pedir alcance (ciudad, entidad, subtipo).
- Orden principal para oportunidades: fecha_de_publicacion_del DESC (frescura). Valor es secundario.
- Si la query es amplia, sugerir acotar por frescura, lugar o subtipo antes de ejecutar consulta costosa.

---

## Matriz de consultas (Fase 2)

Mínimo 30 queries de prueba (incluye el caso semilla y variaciones):

1. que convocatorias hay de mantenimientos
2. convocatorias de mantenimiento
3. licitaciones abiertas de mantenimiento
4. oportunidades de obra en Atlántico
5. procesos abiertos de PAE
6. convocatorias de seguros en Antioquia
7. mínima cuantía publicada esta semana
8. oportunidades en Chiriguaná
9. procesos de ICBF Cesar
10. convocatorias de la Gobernación del Atlántico
11. licitaciones abiertas en Barranquilla
12. procesos de mantenimiento vial
13. oportunidades de mantenimiento locativo
14. procesos publicados este mes
15. convocatorias cerradas
16. licitaciones de alimentación escolar en Boyacá
17. oportunidades de infraestructura en Cundinamarca
18. procesos abiertos de la Alcaldía de Medellín
19. convocatorias de consultoría en Bogotá
20. mínima cuantía de obras en Santander
21. oportunidades de PAE en La Guajira
22. procesos de seguros en Valle del Cauca
23. licitaciones viales publicadas en 2026
24. convocatorias de la Gobernación de Antioquia
25. oportunidades de mantenimiento en Barranquilla
26. procesos abiertos de ICBF en Cesar
27. licitaciones de obra pública en Atlántico
28. convocatorias de servicios de salud en Nariño
29. oportunidades sin fecha en Antioquia
30. procesos de mantenimiento publicados la última semana
31. que contratos hay de mantenimiento (debe ir a dataset contratos, no opportunity_search)
32. adjudicados de obra en Atlántico (excluido)

---

## Tests (Fase 3)

Archivo: tests/test_opportunity_queries.py (mocks, sin red)

Tests mínimos implementados:

1. “oportunidades de mantenimiento” → dataset=procesos
2. “convocatorias de mantenimiento” → estado_family=oferta_abierta
3. “licitaciones abiertas de obra en Atlántico” → procesos + territorio Atlántico
4. “convocatorias de la Gobernación del Atlántico” → entidad (no departamento)
5. Timeout en opportunity_search no debe retornar total_count=0 como si fuera verdad
6. Timeout suggestions no deben sugerir quitar fecha si no había fecha
7. Oportunidad amplia sin fecha/lugar debe sugerir acotar por frescura, lugar o subtipo
8. Query normal de contratos no activa opportunity_search

---

## Reporte (Fase 4)

### Qué ya funciona
- Parser detecta correctamente “convocatorias” / “oportunidades” → dataset=procesos + estado_family=oferta_abierta.
- Entity resolver distingue entidad vs departamento (ej. Gobernación del Atlántico).
- Orden por fecha es default en procesos.

### Qué falla (observado en seed case)
- Timeout tratado como 0 resultados.
- Sugerencias de “quitar fecha” cuando no existe filtro de fecha.
- Ausencia de intent opportunity_search diferenciado.
- Búsquedas amplias sin acotación temporal/lugar generan timeouts o resultados excesivos.
- Orden por valor puede activarse incorrectamente en queries de oportunidades.

### Bugs nuevos identificados (documentados, no fix)
- Ninguno nuevo en esta auditoría (solo seed case previo).
- Falta de ventana temporal implícita controlada para queries de oportunidad amplias.

### Política de oportunidad propuesta
- Intent opportunity_search explícito cuando verbos = convocatorias, oportunidades, licitaciones abiertas.
- Dataset forzado a procesos + estado_family oferta_abierta.
- Orden por fecha_de_publicacion_del DESC.
- Si no hay filtros de lugar/fecha → aplicar ventana 45 días o pedir clarificación.
- Timeout → mensaje específico “SECOP tardó en responder” + reintento sin orden pesado + sugerencias de acotación.

### Cambios mínimos requeridos para OPP-002
- Añadir intent_type=opportunity_search en query_router.
- Modificar soql_builder para priorizar fecha en este intent.
- Añadir guard de ventana temporal en orchestrator cuando opportunity_search y sin fecha/lugar.
- Actualizar formatter para sugerencias específicas de oportunidad.
- Tests de timeout + opportunity ya cubiertos en test_opportunity_queries.py.

---

## Validación

```bash
make lint-core
python -m pytest tests/test_opportunity_queries.py -q
python -m pytest tests/ -q
shasum -a 256 data/feedback.jsonl
git diff --stat
```

**Commit planeado:**
git add docs/OPPORTUNITY_AUDIT.md tests/test_opportunity_queries.py
git commit -m "test: add opportunity hunting audit matrix"