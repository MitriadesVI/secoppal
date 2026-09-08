# Adjudicación acotada de dos huellas — segunda corrida

Orden explícita del usuario: cerrar únicamente las dos guardas pendientes,
sin runtime, sin reconstruir B.3/reconciliación y con una nueva corrida serial
si la adjudicación y sus controles pasan. Se leyeron AGENTS.md y CARRILES.md.

Inicio: 2de487228a091842a3a553f0db503f78f44008a4, limpio, sin trabajo posterior.
Main observado: 7868664757b28bed16c583ea50327d669f1501e3.
Runtime auditado: 4edc248e60610efdbeebb3cb8010bc7b59445cad.
Los dos archivos y todo app/ siguen byte a byte iguales al runtime auditado.

## Autorización y hunks completos

Fuentes de alcance: B.1 REPAIR_DESIGN.md, fila F6 (continuation, admission,
pagination, context y UI), y REMEDIATION.md, matriz F6/F7 y fila UI. La
adjudicación no usa el GO de B.3 como sustituto de los controles. El diff
completo main→candidato de los dos archivos se conserva en production_hunks.diff.gz.
No hay otros hunks en ese intervalo.

| Hunk | Antes | Divergencia B/B.1 autorizada | Control que la delimita |
|---|---|---|---|
| pagination.py:149 | Entrar sólo por frase/grupo; leer params del historial | ff2a5844: aceptar el referente admitido, usar sus params y no releer historial como autoridad competidora | test_bare_more_uses_the_same_tabular_cursor_as_the_full_phrase; test_pagination_consumes_the_published_capability (6 daños); test_ui_reference_is_checked_on_real_reload |
| pagination.py:176 | Buscar buffer por grupo o params legacy | ff2a5844: consumir el buffer ya validado, su grupo, query_params y fingerprint; mantener la rama legacy cuando falta referente | test_validated_referent_keeps_legacy_universe_and_order reutiliza sin modificar 3 controles de test_pagination_buffer con Frame ON; el módulo original corre OFF; agotado, TTL y dual/grupo cubiertos por controles B.1 |
| streamlit_app.py:430 | No había helper de referencia publicada | 20aea784 + ff2a5844: transportar result_id/revisión y chat opcional, sin atribuir autoridad a filtros o metadata | test_ui_reference_transports_only_published_identifiers (5 entradas), ejecuta el helper real extraído por AST |
| streamlit_app.py:448 | Item de historial UI sin referente explícito | 20aea784: añadir continuation_ref al item, preservable en JSON | Mismo control ejecuta _assistant_history_item real y serializa/recarga su salida; test_streamlit_operational_state conserva el render |
| streamlit_app.py:1816 | El chip enviaba params refinados sin referente | 20aea784 + ff2a5844: añadir _continuation_ref del mismo item elegido, sin cambiar apply_refinement ni filtros | test_actual_ui_chip_reference_is_consumed_on_real_reload (válida, ausente, incompleta, vieja, chat ajeno); ejecuta bloque UI real y workflow real, con efectos externos sintéticos |

Precisión sobre el comentario del último hunk: el código anterior ya recorría
el historial al revés y paraba en el primer item con debug. Ese recorrido no
cambia. El cambio efectivo es tomar/transportar también la referencia del mismo
item; no se acredita una nueva política de recorrido por el comentario.
El control de contexto más reciente vacío prueba que no se recupera una
búsqueda más antigua para inventar un referente.

La validación del referente precede al handler: validate_tabular coteja
capacidad publicada, contenido del buffer, generación, fingerprint, contrato
de orden, grupo y TTL. El handler consume ese buffer y copia sus params.
Con Frame OFF/sin referente, las ramas get_history/get_pagination_buffer y
el algoritmo restante son los mismos. F6 permite cambiar quién autoriza la
continuación; no permite cambiar los filtros, el orden ni el universo.

## Invariantes preservadas

- Filas sin solapamientos ni omisiones; orden concatenado y desempates estables.
- Exclusiones/filtros persistentes, sin expansión de compradores después del fetch.
- Recarga del buffer sin refetch; agotamiento sin consultas; selección de grupo
  sólo dentro del universo publicado; TTL/capacidad inválidos bloqueados.
- Semántica de apply_refinement intacta. Referencia UI acompaña a los mismos
  params; identificación inválida no autoriza ejecución y una válida sí funciona.
- Oráculos públicos históricos de SoQL, keys/tipos/serialización, ranking,
  conteos, presentación M2/M2.1, estado operativo y no activación permanecen.

Se añadieron 14 pruebas focales porque faltaba cobertura directa del transporte
UI y faltaba ejecutar los controles exactos de orden/universo con referente
validado. Se extraen AST de funciones/bloque reales de Streamlit siguiendo el
patrón existente del repo; no se arranca la app. Tres casos invocan los cuerpos
intactos de controles legacy bajo la fixture frame_v12. No se añade una
propiedad ornamental: la propiedad existente de referencias ajenas se ejecutó.

## Decisión condicionada ejecutada

Los 60 controles pasaron offline antes del repin. La autorización condicionada
queda satisfecha para esas dos entradas. Los hashes se calcularon sobre los
archivos comparados contra el runtime auditado, no se copiaron de una tabla.
pin_update.json conserva old/new y acredita AST de todas las funciones de
la guarda idéntico y todas las demás huellas idénticas.

Después del repin: módulo completo de paridad 14 passed. Ruff y Semgrep
focales se registran aquí; Semgrep conserva 10 hallazgos heredados y cero
nuevos. La primera corrida roja y sus diagnósticos permanecen intactos.

## Runner y aislamiento de la nueva corrida

Se reutiliza run_gate.py con el parámetro --output-dir. Éste es el único
ajuste mecánico al runner: dirige logs, XML, JSON, semgrep y observaciones a
round2/. Mantiene los mismos bloques activos, globs de Ruff complementario,
comparador Semgrep, flags, restricciones de red y acumulación de resultados.
La ruta de salida debe quedar dentro del directorio del gate; se conserva el
rechazo a sobrescribir una corrida existente. No se duplica el runner.

```sh
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python docs/audits/entity_profile_frame_v1_2_coordinator_gate/run_gate.py full --output-dir docs/audits/entity_profile_frame_v1_2_coordinator_gate/round2
```

Los node IDs complementarios y sus fixtures son los mismos de la primera
corrida. La única ampliación de colección global son los 14 controles de este
cierre. Sin runtime, otros hashes, límites, reglas, dependencias ni marcas
skip/xfail nuevas. El P2 de recuperación y las observaciones diagnósticas de
reset conservan exactamente sus límites; no se reabrieron F4/proveedores.

## Preflight

Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10. Codegraph no inició con
Node 26.8.1; salida de versión/status en preflight.json. Sin instalaciones.
Fallback rg, Git y lectura manual. No se confió en un índice no comprobado.
El SHA nuevo y los resultados globales se registrarán tras congelar/ejecutar;
la validación focal previa corresponde al árbol preparado aún sin commit.
