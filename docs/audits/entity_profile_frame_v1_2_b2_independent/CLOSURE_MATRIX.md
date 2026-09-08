# Matriz de cierre y regresión

La matriz cita funciones; `reconciliation_manifest.json` contiene los node IDs parametrizados exactos y el estado de cada target. `I` = workflow y recarga reales; `A` = API; `P` = lector/ejecución en procesos; `H` = propiedad Hypothesis determinista. Todo el I/O externo de negocio es sintético.

| Familia | Frontera y controles | Resultado independiente |
|---|---|---|
| F4 | I/P: preparación, append parcial/completo, fsync sobre/directorio, arm write/fsync/directorio, unlink antes/después, rearme fallido, reset/épocas, chat distinto, nueva publicación | Originales y B.2 verdes. Dos secuencias propias rojas: prefijo fallido pierde su barrera al revertir el siguiente intento. **PARCIAL/P1**. |
| F5 | I: 20 daños individuales B.1, seis daños propios con multiplicidad/membresía y digest coherente; H: duplicación intentada/no intentada; I: éxito, parcial, total, pending y BUDGET_EXHAUSTED | Todos verdes. El prefijo dañado queda intacto; nueva aclaración permitida sin consultas. **CERRADO**. |
| F9 | A: otro comprador con mismos IDs/predicados; copia serializada igual; BuyerUnit/BuyerGroup y kind; I: 14 daños de snapshot/digest, incluido vínculo ausente | Todos verdes. No se reconstruye autoridad faltante. **CERRADO**. |
| Proveedores | I/A: Infinity, 1e999, -Infinity, NaN, None, vacío, texto malformado, 1e-999; secciones/registro/respuesta/recarga; values; montos válidos hasta 1e308 como control de representación | Overflow y persistencia honestos en los casos simétricos. Dos rojos en TOPs distintos: afirmación y advertencia contradictorias. **PARCIAL/P2**. |
| F1 | I: `test_clarification_preserves_the_profile_it_offers_to_continue`; ordinal sin opciones, repetición/chip, pending que conserva ejecución; H de referencias | Controles vigentes verdes. **CERRADO conservado**. |
| F2 | I: reformulación completa con/sin confirmado y comprador igual/distinto; cancelación; R5d y cambios incompletos sin llamadas | Históricos vigentes/ports verdes. **CERRADO conservado**. |
| F3 | I/P: LIST/RUN, lookup, Actor documento y token; API y control directo con tokens propios; replay, TTL, versión, identidad, OFF y owner Radar | Históricos, ports y controles B.1 verdes. Tres ports no reabiertos. **CERRADO conservado**. |
| F6 | I: seis daños de cursor, TTL, más/grupos/exhaustion; UI ref ausente/incompleta/vieja/ajena/válida; dueño delegado explícito | Históricos y controles B.1 verdes. **CERRADO conservado**. No invalida el hallazgo distinto F4, que revive una capacidad de un turno fallido. |
| F7 | I: OFF→ON, migración equivalente, contextos incompatibles bloqueados; tabla→perfil→OFF; LIST/RUN OFF | **CERRADO conservado** para este binario con guarda. No certifica rollback a un binario antiguo. |
| F8 | P/A: sin fcntl ambos procesos rechazan antes de entrar; con flock hay exclusión real; ejecución serial y otro chat | Ports y controles vigentes verdes. **CERRADO conservado**; filesystem compartido, sin promesa distribuida. |
| F10 | I/A: SAVE tras perfil sin efectos; export de tabla vigente; owner y filtros exactos; LIST/RUN con runner sintético | Históricos y controles B.1 verdes. **CERRADO conservado**; sin servicios Radar reales. |

Los 70 node IDs vigentes de la auditoría B y sus cuatro controles verdes apartados están individualmente comprobados en `historical_current.xml.gz`; el target completo con ports tiene 105 verdes. Los once residuales originales y los 45 controles independientes B.1 se comprobaron por identidad dentro de los 130 verdes de aceptación. Los 53 node IDs nuevos del autor B.2 permanecen propuestos como activos; no se archivan junto a los montajes antiguos.

| Regresión dirigida adicional | Resultado / alcance |
|---|---|
| A2/R3d y 13 contraejemplos | Ejecutados en amplio y núcleo; contraejemplos verdes. La única falla del núcleo es el montaje del reloj descrito abajo. |
| R1/R2/R4, ocho escenarios B, seis focos | Targets B y utilidad vigentes; mantienen SQL/fuentes y herencia por recarga. |
| Presupuestos | Seis focos y 4/2/1 en pruebas B.2; own suppliers 4/2. Sin llamadas adicionales para validar/renderizar/recargar. |
| CLEAR, exclusiones y reloj inyectado | Controles del workflow verdes. `test_the_injected_clock_is_the_one_that_resolves_relative_periods` pasa. |
| Fuente parcial/total y observaciones no intentadas | Publicación real y continuación válidas. El estado de ejecución no se “repara” a éxito. |
| Analytics y consumidores monetarios | Consumidor `test_entity_profile_metric_evidence.py` incluido; `values` responde igual con suppliers inválidos. No se generaliza a todo analytics. |
| Adyacentes | 472 verdes, dos xfails Actor preexistentes, sin xfails nuevos. |
| Histórico temporal dependiente del día | `test_a_relative_period_does_not_get_reinterpreted_on_reload` rojo tanto en candidato como base B.1 el 2026-09-08. Deuda del montaje; no nuevo defecto B.2. Sigue activo para corrección del coordinador. |

**No sumar conjuntos.** Los 47 verdes propios finales incluyen dos observaciones de PublicationRecoveryError que no acreditan seguridad. La campaña nueva acredita nueve retiradas conductuales específicas con baseline verde; no convierte los cuatro cierres completos en GO y no sustituye los contraejemplos propios.
