# CONTRACT_UNIVERSE_V1_READY_FOR_REVIEW

La política común se aplica antes de recuperar filas, paginar y agregar. La
causa era tratar el dataset de contratos como prueba de celebración: Search y
sus agregados no tenían elegibilidad, y Frame congelaba otro WHERE. Ambos
caminos ahora proyectan `ContractUniverse`, con una lista positiva compartida,
acceso explícito a registros y compatibilidad de continuaciones.

**SHA probado:** `d63ce7e9f33831ac2bb8a6e773078e4d8964c902`.
Base integrada y `main`: `c52d7eda355f44278164d4cb9803a199eec83486`.
Rama: `fix/contract-universe-v1`.
El HEAD de entrega añade solamente este paquete de evidencia al SHA probado;
se comunica completo en el mensaje de entrega. No hubo merge ni push.

## Estados y consumidores

La [tabla de estados y adjudicación de Aprobado](POLICY.md) distingue el gestor
nativo del dataset. Aprobado se incluye por decisión operativa apoyada en el
catálogo versionado y diez muestras públicas; **la inclusión sin fecha es una
inferencia de política, no una universalidad demostrada por esas muestras**.
No se certifica jurídicamente cada expediente ni se infieren fechas/años.

Quedaron cubiertos Search/count, sumas/rankings, observer y clientes derivados,
los ocho aspectos del Actor, Entity con Frame OFF/ON, Frame congelado y su
publicación, consultas duales, páginas, exportación, Radar y consulta directa.
El mapa exacto está en POLICY.md. Los radares, exportaciones, buffers y perfiles
antiguos incompatibles requieren revisión; no se borran datos. Una búsqueda
completa nueva sigue disponible. Un perfil de registros que ya acredita esta
política mantiene su significado al pasar por la conversión legacy → Frame.

## Diferencia de producto

Fixture SQL ejecutable: 8 admitidos, 55 excluidos y dos controles fuera del
comprador/monto. Incluye un histórico fechado en 2025, dos admitidos sin fecha,
y registros excluidos con/sin fecha y valores enormes. Comparten URL sin que
se deduplique por ella.

| Semilla exacta | Antes | Después | Suma contractual después |
|---|---:|---:|---:|
| que ha contratado alcaldia de barranquilla por mas de 2000 millones | 63 registros | 8 contratos | 16.000.000.800 COP |
| contratos de alcaldia de cartagena por mas de 500 millones | 63 registros | 8 contratos | 4.000.000.800 COP |

IDs admitidos: `VALID-0` a `VALID-7`. La primera ventana antigua deja entrar
registros excluidos; filtrarlos después de LIMIT pierde históricos válidos.
Conteos, sumas y rankings contractuales se mantienen al insertar/reordenar
excluidos. El universo explícito discrimina el control inverso: conserva 63
registros; «solo 2026» conserva 35, frente a 5 en el universo celebrado.

Páginas sin solapamiento tras recarga, comprador/monto conservados, metadatos
XLSX preservados. Cancelados explícitos: 10; refinados a 2026: 5. Suspendido no
trae Cancelado. Actor con sólo excluidos sigue resolviendo y muestra cero
contratos en el alcance. Radar contractual: 8 inicialmente y 7 después de una
cancelación; el seguimiento por ID conserva el cambio fuera del conteo.

La conversión de un perfil de registros Frame OFF → ON tuvo un rojo real de
**5 frente a 35** y quedó corregida. `migration_red.txt` y
`migration_green.txt` conservan esa diferencia. Las aserciones de métricas
identifican el agregado global, no el primer grupo de proveedores recibido.

## Verificación literal

Corrí 1.165 tests dirigidos de los consumidores, regresiones Buyer–Actor,
corpus y controles Frame enumerados en `final_validation.json`, no la suite.
La aceptación específica volvió a ejecutar 34 controles en el mismo SHA para
producir `product_trace_d63ce7e.jsonl` con SQL, filas, respuestas y publicación.

```text
All checks passed!
Ran 3 rules on 330 files: 10 findings.
1165 passed in 64.56s (0:01:04)
Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
app/core/ limpio
```

Semgrep: **baseline 10, candidato 10, nuevos 0, resueltos 0**. No es un scan
limpio. Los módulos nuevos aparecen en `paths.scanned`. Ruff también comprobó
`streamlit_app.py`. `git diff --check` y el control de tamaño pasaron.

Comandos, códigos de salida, stdout/stderr completos y comparación con base:
[final_validation.json](final_validation.json). Salidas previas literales:
[literal_logs.json](literal_logs.json). Herramientas, alcance de Codegraph CLI,
respaldo `rg`, espacio medido y hashes de árboles: [PREFLIGHT.json](PREFLIGHT.json).

Mutaciones en copias desechables: **4/4 detectadas** con baseline verde:
COUNT sin política, compilador Frame sin política, exclusión después de LIMIT,
y pérdida del universo en recarga. Son diferencias de IDs/conteos, no errores
de importación, timeouts o cambios de leyenda. La base integrada reproduce los
cuatro fallos de las semillas sin intentos externos.
[mutations.json](mutations.json) contiene los comandos y aserciones.

Los cambios de expectativas están adjudicados en [EXPECTATIONS.md](EXPECTATIONS.md).
Sólo se repinearon ocho entradas de archivos, con hunk/causa/control en
[repins.json](repins.json); los oráculos de salida de procesos siguen intactos.

## Fuera del carril y límites

Los diez hallazgos históricos de Semgrep no se corrigieron. No se rediseñaron
ranking, deduplicación, montos anómalos, modelos, identidad ni publicación P2.
No se prometen conteos reales iguales a capturas: los números anteriores son
los de la fuente sintética controlada. Los dos agregados públicos vencidos no
se convierten en cero ni en conclusiones nacionales.

No quedó pendiente un consumidor declarado del universo de descubrimiento.
La decisión operativa sobre Aprobado, especialmente sin fecha, debe examinarse
con su límite de evidencia explícito durante la revisión. Quedan fuera de esta
entrega la revisión independiente, suite global del coordinador e integración.
No hubo reinicio de aplicaciones, flags productivos, despliegue, merge ni push.
