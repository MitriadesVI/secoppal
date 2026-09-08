# Gate coordinador Frame V1.2

Estado: candidato en preparación; el resultado definitivo se añadirá tras el
gate sobre SHA congelado. No se ha integrado ni activado el Frame.

## Alcance y preservación

AGENTS.md, CARRILES.md, diseño congelado y las adjudicaciones B/B.1/B.2/B.3
consultados. La asignación explícita del usuario autoriza este carril y una
suite global serial. Sin app, merge, push, despliegue, runtime ni reglas nuevas.

`reconciliation.json` conserva commit, SHA256 y archivo original→activo;
`case_map.json` clasifica sólo casos Frame afectados/complementarios.
Los originales de los cinco archivos modificados de tests/ están archivados
byte a byte con extensión `.original` fuera de colección. Los originales
históricos B.2/B.3 y su evidencia permanecen en sus rutas, sin modificaciones.
Las copias activas preservan fixtures, helpers y aserciones vigentes; las
variantes sustituidas se retiran de parametrización, sin deselects ni xfails.

Tres ports B.1 y dos expectativas F5 usan exactamente las adjudicaciones
referenciadas por B.3. En B.3 se conservan los controles positivos de B.2 y se
ejecutan sólo los ports complementarios correspondientes, sin contar dos veces
las variantes equivalentes. No se archivan completos archivos mixtos.

## Reloj y reset

El helper pasaba TODAY a QueryRouter y no a collect_claims. Ahora propaga la
misma fecha explícita a ambos. Se conserva la aserción histórica 2026-09-07 y
se añaden dos confirmaciones en fechas distintas con continuación/recarga
posterior; su periodo confirmado no cambia. Se conservan todos los controles
del reloj del workflow real y las propiedades Hypothesis existentes.

El nuevo control ejecuta A confirmado → B con fsync fallido → /reset real →
lectura desde otro proceso → nueva consulta → continuación. Observa que el
handler real llama ConversationStore.clear; exige ausencia de sobre/historial
tras reset y que la publicación nueva sea la solicitada, sin B ni continuidad
anterior. No redefine el conjunto de confirmados desde lo que quedó legible.

La primera versión del nuevo test exigía historial legacy no vacío tras un
perfil publicado: supuesto erróneo del montaje, pues el perfil usa su sobre.
Se conservó el rojo en focal_focused.log y se corrigió para exigir el sobre
actual y la ausencia de B. El reset se repitió focalmente, sin tocar runtime.

## P2 y límites de operación

P2-1 de B.3 sigue presente: si falla unlink después de hacerse visible, falla
el rearme y además falla escribir la marca después de truncate, puede quedar
admitido B, un registro anterior expresamente no publicado. Se lanza
PublicationRecoveryError. La excepción no certifica bloqueo ni inmunidad
física. No es sólo el caso de que quede íntegro el sobre nuevo.

Registrar este límite permite continuar el gate para solicitar integración
con Frame OFF. No es PRODUCTION_GO ni aceptación de riesgo para terceros.
El control de /reset usa almacenamiento recuperado después del fallo
inyectado: no demuestra que un reset siempre funcione con almacenamiento
averiado. probe_reset.py sólo llama advance_epoch; no ejecuta handle_reset
ni ConversationStore.clear. Su observación no sustituye el control de comando.

## Instrumentación

Preflight: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10. Codegraph CLI
rechazó Node 26.8.1 al solicitar versión/status; no se confió en su índice ni
se declaró MCP activo. Fallback explícito a rg, lectura de consumidores/helpers
y Git. Se añadieron controles de fechas, no una propiedad ornamental para
el cambio mecánico del helper. Las propiedades existentes se ejecutan en el gate.

## Pendientes antes de integración

Resultado global, excepciones y SHA final se consignarán después de ejecutar.
Cualquier merge, push, dogfood, despliegue o activación requiere decisión
posterior. La integración deberá comprobar nuevamente main, preservar Frame
OFF y reflejar el resultado real en la bitácora canónica, sin anticiparlo.
