# Adenda de contrato para follow-ups (decisiones de producto, 2026-10-01)

Decididas por el usuario. Son REGLAS GENERALES: rigen el etiquetado igual que el contrato M1_0 v1.4. No dicen la
etiqueta de ningún caso concreto.

## Composición
F1. Un follow-up es un conjunto atómico de operaciones `SET / CLEAR / KEEP / SET_ORDER / SET_UNIVERSE`. Lo no mencionado
    se conserva. Una condición arrastrada sólo desaparece con una remoción CITADA en el follow-up.
F2. VALUE se lee por LADO del intervalo: una cota inferior nueva reemplaza la inferior; una superior nueva reemplaza la
    superior; una cota del lado que faltaba completa el rango. Si la cota nueva deja el rango SIN INTERIOR (vacío o de un
    solo punto), la restricción previa se sustituye entera y se anuncia (`effects: VALUE_RANGE_REPLACED`). Una instrucción
    explícita de cambio («cambia el valor a…») sustituye la restricción.
F3. Un TOPIC nuevo REEMPLAZA al previo (`effects: TOPIC_REPLACED`), anunciado.
F4. Re-consulta: «los de mayor valor», «aquellos de…» re-consultan con la condición añadida; no filtran sólo lo mostrado.

## Herencia (la hace el host, nunca el intérprete; siempre anunciada en el eco)
F5. Monto sin unidad: hereda la unidad sólo si la condición VALUE previa que se modifica tiene una única unidad activa
    (`unit_source = PREVIOUS_ACCEPTED_VALUE`). Si no, CLARIFY.
F6. Unidad dicha una vez en un rango («entre 200 y 500 millones»): cubre las dos cotas (`unit_source = SIBLING_IN_RANGE`).
F7. Mes sin año: hereda el único año activo del TIME previo (`year_source = PREVIOUS_ACCEPTED_TIME`). Sin TIME previo:
    ocurrencia más reciente no posterior a hoy (`CLOCK_INFERRED`), anunciada. Si el TIME previo abarca varios años o es una
    ventana relativa: CLARIFY.
F8. Rango de años «de A a B» / «entre A y B»: inclusivo, el año final entra COMPLETO.

## Vista, orden y cardinalidad
F9. La vista por defecto se recalcula tras un delta sólo si era DERIVADA (universo no dicho); se anuncia
    (`effects: VIEW_REDERIVED`). Un universo dicho por el usuario o elegido por botón no se recalcula.
F10. Top-N arrastrado: si había «los N más…» y llega un orden nuevo («ordénalos por fecha»), se conserva N y se reordena
     según lo pedido (más reciente o más antiguo), anunciado.

## Vigencia del contexto
F11. Hasta 30 minutos desde el turno previo, un fragmento es continuación. Pasado ese plazo NO se ejecuta directo ni se
     descarta el contexto en silencio: la disposición esperada es `CLARIFY` con `disposition_reason =
     CONTEXT_EXPIRED_CONFIRM`, que ofrece de un toque «continuar sobre la búsqueda anterior» o «reset». `resulting_ir` se
     etiqueta igualmente como lo que resultaría al continuar.

## Ambigüedades que se preguntan
F12. «con X» tras un ACTOR, cuando X puede ser comprador o lugar: `CLARIFY` con las dos opciones (comprador / lugar).
F13. Región: lista CERRADA de regiones definidas por departamentos (la ya establecida en el producto). Un nombre de región
     fuera de la lista: `CLARIFY`.
F14. Monto en SMMLV: no ejecutable hoy; disposición `CLARIFY` con `disposition_reason = SMMLV_UNSUPPORTED` (default
     provisional; el usuario no lo ha confirmado expresamente).
F15. Operadores relativos a un valor previo («el mes siguiente», «bájalo», «el doble»): `m2a_scope =
     OUT_RELATIVE_TO_PREVIOUS`, `CLARIFY`.

## Sin decidir (usa el `gap_code`, etiqueta tu mejor lectura por contrato y explica la alternativa)
- Cota inferior sin comparador dentro de un rango (`B5RANGE`).
- Comparador con las palabras desordenadas (`COMPARATOR_FORM`).
- Cambio de operación por follow-up y palabra sola tras un conteo o un ranking (`OPSWITCH`, `TOTALES`, `FIRMADOS`): fuera
  de M2a.

## Decisiones añadidas el 2026-10-01 (segunda tanda; el usuario respondió «todo ok»)
F14 (reemplaza a la provisional). SMMLV NO es ejecutable en ningún turno, tampoco en el primero: el producto lo dice y
     pide el valor en pesos (`SMMLV_UNSUPPORTED`). Tarea futura del carril de dinero: hacerlo ejecutable con una tabla de
     salario por año con dueño (un solo año en la búsqueda ⇒ salario de ese año; si no, el del año en curso; cifra en
     pesos siempre anunciada).
F16. Negar el único valor positivo vigente de una ranura («que no sean de aseo» con TOPIC = aseo): se INVIERTE
     (exclusión de ese valor), anunciado.
F17. Negar un valor distinto del vigente: se suma como exclusión; el positivo se conserva.
F18. `CLEAR` sobre una ranura vacía: no re-consulta; responde que ese filtro no estaba.
F19. Un delta que dejaría la búsqueda sin ninguna condición: `CLARIFY`.
F20. Top-N por encima de 100: ejecuta con 100 y lo anuncia (`TOPN_CAPPED`); no pregunta.
F21. Orden por fecha sin dirección: más reciente primero, anunciado.
F22. Tema desnudo tras una búsqueda («primera infancia» tras «Alcaldía de Barranquilla»): es DELTA; el tema se fija o
     reemplaza (F3) y todo lo demás se conserva, con eco de lo arrastrado.
F23. «No, mejor X» con aclaración pendiente: cancela la pendiente; si X es un fragmento, es delta sobre la interpretación
     anterior; si es una consulta completa, es búsqueda nueva.
F24. «Cualquiera» ante una aclaración con opciones: `REASK` una vez; el producto no elige.
F25. Respuesta a una aclaración que además edita otra ranura: `REASK` nombrando la salida; nunca un híbrido.
F26. Unidad heredada que produce un rango sin interior o una magnitud absurda: `CLARIFY` (no se encadenan herencia y
     reemplazo).
F27. Estado que no existe en el universo pedido: `CLARIFY`, ofreciendo cambiar de universo.
F28. Fragmento sin búsqueda previa: se pide la búsqueda completa.
F29. Follow-up sobre un turno que no se ejecutó: si responde la pregunta pendiente, se aplica; si no, `REASK`.
F30. Territorio nuevo con un comprador de otro territorio arrastrado: `CLARIFY` (¿cambia de comprador o busca en ambos?).

Corrección de diseño (no es decisión de producto): la aclaración pendiente debe guardar el span ambiguo que la originó.

## Idea registrada, NO decidida (2026-10-01)
«Volver atrás» en follow-ups: como PROTOCOLO (botón y frases cerradas, sin LLM) sobre el almacén de aceptadas, con dos
acciones — deshacer el último paso y volver a la búsqueda inicial —, siempre con eco. El diseño v2 lo lista fuera de
M2a. Sin evidencia de uso en los 65 reales. Si se adopta: añadir a la adenda y ~8 casos sintéticos.
