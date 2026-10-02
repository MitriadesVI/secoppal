# Adenda de contrato para follow-ups — versión consolidada (v2)

Decisiones de producto tomadas por el usuario el 2026-10-01 (dos tandas), consolidadas y limpias. Sustituye a
`kit/CONTRACT_ADDENDUM_FOLLOWUPS.md` como texto de referencia para etiquetar. Son REGLAS GENERALES: rigen el etiquetado
igual que el contrato M1_0 v1.4 y sus enmiendas. No dicen la etiqueta de ningún caso concreto.

Esta consolidación no cambia el sentido de ninguna regla ni añade reglas: sólo quita la F14 provisional (queda la que la
reemplazó) y ordena F16–F30 en su sección. Las reglas que chocan entre sí o que admiten dos lecturas están al final,
en «Tensiones», sin resolver.

## Composición
F1. Un follow-up es un conjunto atómico de operaciones `SET / CLEAR / KEEP / SET_ORDER / SET_UNIVERSE`. Lo no mencionado
    se conserva. Una condición arrastrada sólo desaparece con una remoción CITADA en el follow-up.
F2. VALUE se lee por LADO del intervalo: una cota inferior nueva reemplaza la inferior; una superior nueva reemplaza la
    superior; una cota del lado que faltaba completa el rango. Si la cota nueva deja el rango SIN INTERIOR (vacío o de un
    solo punto), la restricción previa se sustituye entera y se anuncia (`effects: VALUE_RANGE_REPLACED`). Una instrucción
    explícita de cambio («cambia el valor a…») sustituye la restricción.
F3. Un TOPIC nuevo REEMPLAZA al previo (`effects: TOPIC_REPLACED`), anunciado.
F4. Re-consulta: «los de mayor valor», «aquellos de…» re-consultan con la condición añadida; no filtran sólo lo mostrado.
F22. Tema desnudo tras una búsqueda («primera infancia» tras «Alcaldía de Barranquilla»): es DELTA; el tema se fija o
     reemplaza (F3) y todo lo demás se conserva, con eco de lo arrastrado.

## Polaridad
F16. Negar el único valor positivo vigente de una ranura («que no sean de aseo» con TOPIC = aseo): se INVIERTE
     (exclusión de ese valor), anunciado.
F17. Negar un valor distinto del vigente: se suma como exclusión; el positivo se conserva.

## Remoción
F18. `CLEAR` sobre una ranura vacía: no re-consulta; responde que ese filtro no estaba.
F19. Un delta que dejaría la búsqueda sin ninguna condición: `CLARIFY`.

## Herencia (la hace el host, nunca el intérprete; siempre anunciada en el eco)
F5. Monto sin unidad: hereda la unidad sólo si la condición VALUE previa que se modifica tiene una única unidad activa
    (`unit_source = PREVIOUS_ACCEPTED_VALUE`). Si no, CLARIFY.
F6. Unidad dicha una vez en un rango («entre 200 y 500 millones»): cubre las dos cotas (`unit_source = SIBLING_IN_RANGE`).
F7. Mes sin año: hereda el único año activo del TIME previo (`year_source = PREVIOUS_ACCEPTED_TIME`). Sin TIME previo:
    ocurrencia más reciente no posterior a hoy (`CLOCK_INFERRED`), anunciada. Si el TIME previo abarca varios años o es una
    ventana relativa: CLARIFY.
F8. Rango de años «de A a B» / «entre A y B»: inclusivo, el año final entra COMPLETO.
F26. Unidad heredada que produce un rango sin interior o una magnitud absurda: `CLARIFY` (no se encadenan herencia y
     reemplazo).

## Vista, universo, orden y cardinalidad
F9. La vista por defecto se recalcula tras un delta sólo si era DERIVADA (universo no dicho); se anuncia
    (`effects: VIEW_REDERIVED`). Un universo dicho por el usuario o elegido por botón no se recalcula.
F27. Estado que no existe en el universo pedido: `CLARIFY`, ofreciendo cambiar de universo.
F10. Top-N arrastrado: si había «los N más…» y llega un orden nuevo («ordénalos por fecha»), se conserva N y se reordena
     según lo pedido (más reciente o más antiguo), anunciado.
F20. Top-N por encima de 100: ejecuta con 100 y lo anuncia (`TOPN_CAPPED`); no pregunta.
F21. Orden por fecha sin dirección: más reciente primero, anunciado.

## Vigencia y existencia del contexto
F11. Hasta 30 minutos desde el turno previo, un fragmento es continuación. Pasado ese plazo NO se ejecuta directo ni se
     descarta el contexto en silencio: la disposición esperada es `CLARIFY` con `disposition_reason =
     CONTEXT_EXPIRED_CONFIRM`, que ofrece de un toque «continuar sobre la búsqueda anterior» o «reset». `resulting_ir` se
     etiqueta igualmente como lo que resultaría al continuar.
F28. Fragmento sin búsqueda previa: se pide la búsqueda completa.
F29. Follow-up sobre un turno que no se ejecutó: si responde la pregunta pendiente, se aplica; si no, `REASK`.

## Aclaraciones pendientes y sus respuestas
F23. «No, mejor X» con aclaración pendiente: cancela la pendiente; si X es un fragmento, es delta sobre la interpretación
     anterior; si es una consulta completa, es búsqueda nueva.
F24. «Cualquiera» ante una aclaración con opciones: `REASK` una vez; el producto no elige.
F25. Respuesta a una aclaración que además edita otra ranura: `REASK` nombrando la salida; nunca un híbrido.

## Ambigüedades que se preguntan
F12. «con X» tras un ACTOR, cuando X puede ser comprador o lugar: `CLARIFY` con las dos opciones (comprador / lugar).
F13. Región: lista CERRADA de regiones definidas por departamentos (la ya establecida en el producto). Un nombre de región
     fuera de la lista: `CLARIFY`.
F14. SMMLV NO es ejecutable en ningún turno, tampoco en el primero: el producto lo dice y pide el valor en pesos
     (`SMMLV_UNSUPPORTED`). Tarea futura del carril de dinero: hacerlo ejecutable con una tabla de salario por año con
     dueño (un solo año en la búsqueda ⇒ salario de ese año; si no, el del año en curso; cifra en pesos siempre anunciada).
F15. Operadores relativos a un valor previo («el mes siguiente», «bájalo», «el doble»): `m2a_scope =
     OUT_RELATIVE_TO_PREVIOUS`, `CLARIFY`.
F30. Territorio nuevo con un comprador de otro territorio arrastrado: `CLARIFY` (¿cambia de comprador o busca en ambos?).

## Corrección de diseño (no es decisión de producto)
La aclaración pendiente debe guardar el span ambiguo que la originó.

## Sin decidir (usa el `gap_code`, etiqueta tu mejor lectura por contrato y explica la alternativa)
- Cota inferior sin comparador dentro de un rango (`B5RANGE`).
- Comparador con las palabras desordenadas (`COMPARATOR_FORM`).
- Cambio de operación por follow-up y palabra sola tras un conteo o un ranking (`OPSWITCH`, `TOTALES`, `FIRMADOS`): fuera
  de M2a.

## Idea registrada, NO decidida
«Volver atrás» en follow-ups: como PROTOCOLO (botón y frases cerradas, sin intérprete) sobre el almacén de
interpretaciones aceptadas, con dos acciones — deshacer el último paso y volver a la búsqueda inicial —, siempre con eco.
No es regla: no se etiqueta. Si se adopta, se añade a esta adenda y se escriben casos sintéticos.

## Tensiones (reglas que chocan o admiten dos lecturas; sin resolver aquí)

| Id | Reglas | Qué choca o qué falta |
|---|---|---|
| X1 | F23 ↔ F29 | Una reformulación con rechazo («no, mejor X») no responde la pregunta pendiente. F29 manda `REASK`; F23 manda cancelar y aplicar. Falta decir que F23 prevalece cuando hay rechazo explícito, y qué cuenta como rechazo explícito. |
| X2 | F23 | «Delta sobre la interpretación anterior»: no dice si la base es el marco que estaba pendiente (que no se ejecutó) o la última búsqueda ejecutada antes de él. Con un marco pendiente en el primer turno no hay búsqueda ejecutada previa. |
| X3 | F22, F23, F28 | «Fragmento», «tema desnudo» y «consulta completa» no están definidos. Un texto con tema y año pero sin sustantivo de universo, ¿es fragmento o consulta completa? F22 sólo nombra el tema solo. |
| X4 | F29 | «Un turno que no se ejecutó» cubre dos casos distintos: quedó una pregunta abierta, o la interpretación se aceptó y la fuente falló. En el segundo no hay pregunta pendiente que responder ni que volver a hacer. |
| X5 | F2 ↔ F5 ↔ F26 | F5 hereda la unidad; F2 sustituye la restricción cuando el rango queda sin interior; F26 manda `CLARIFY` cuando ocurren las dos cosas a la vez. F26 es la más específica, pero cambia el resultado de una combinación que F2 y F5, leídas por separado, ejecutaban. |
| X6 | F26 | «Magnitud absurda» no tiene umbral. |
| X7 | F16 | «El único valor positivo vigente»: no dice qué pasa si la ranura tiene varios valores positivos y se niega uno de ellos, ni si «el mismo valor» incluye un valor contenido en el vigente (un municipio de un departamento vigente). |
| X8 | F18 ↔ F1 | `CLEAR` sobre ranura vacía «no re-consulta»; F1 dice que el follow-up es atómico. Si el mismo turno trae además otra operación válida, no se dice si se aplica la otra o no se aplica nada. |
| X9 | F19 | «Sin ninguna condición»: no dice si una búsqueda que sólo conserva el universo, el orden o la cardinalidad cuenta como «sin condición». |
| X10 | F20 ↔ ADR-103 P8 | P8 decía «máximo 100, anunciado; por encima, aclarar». F20 dice ejecutar con 100 y anunciar. F20 es posterior; P8 y el comentario del contrato de valores que lo cita quedan desactualizados. |
| X11 | F10 ↔ F21 | Con top-N arrastrado y «ordénalos por fecha» sin dirección: F10 dice «según lo pedido (más reciente o más antiguo)» y F21 fija más reciente primero cuando no se dice. Compatibles, pero F10 no remite a F21. |
| X12 | F30 | «Comprador de otro territorio» no está definido para compradores nacionales, ni para un territorio contenido en el del comprador (municipio dentro del departamento de una gobernación) o que lo contiene. |
| X13 | F12 ↔ F30 | Tras un ACTOR, un lugar introducido por «con»: F12 pregunta comprador o lugar. Si además hay un comprador arrastrado de otro territorio, aplican las dos preguntas y no se dice cuál va primero. |
| X14 | F14 ↔ F5 | F5 permite heredar «la única unidad activa»; F14 hace que SMMLV no sea ejecutable. No se dice expresamente que SMMLV no se hereda (se sigue de F14, pero F5 no lo excluye). |
| X15 | F11 ↔ F29 | Contexto vencido con una pregunta pendiente: F11 ofrece «continuar o reset»; F29 pide responder la pendiente. No se dice cuál se ofrece primero. |
| X16 | F6 y cabecera de Herencia | La cabecera dice «siempre anunciada en el eco». F6 (unidad repartida dentro de un mismo rango) está bajo esa cabecera, pero no es herencia entre turnos: no se dice si exige un anuncio propio o basta el valor completo. |
| X17 | F27 ↔ contrato M1 §18–§19 | F27 es general; las reglas ACTOR×STATE del contrato (v1.8/v1.9) ya redirigen o aclaran por dataset de existencia. No se dice cuál precede cuando hay ACTOR. |
| X18 | F15 | «El año anterior», «el mes pasado» pueden ser relativos al valor previo (F15: aclarar) o al reloj (periodo relativo ejecutable). F15 no da el criterio. |
