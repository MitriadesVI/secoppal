# A1-ADV — Cola de CONTRACT_GAPS (slices MINIMAL_PAIR y ADVERSARIAL)

Etiquetador `sonnet-ADV`. Gold de significado: ninguna etiqueta salió de ejecutar el producto. Cada
punto lista los gold_id afectados y la decisión que se tomó para poder seguir. Todos requieren
decisión del contrato antes de sellar. Referencias al contrato v1.1 (§ y reglas A1–A6, D1–D6).

## G1. Lo que la §5 resuelve contra la intuición (no se reetiquetó, A6)

| Id | Caso | Efecto |
|---|---|---|
| G1a | P033 «pavimentación en el Huila este mes» | PV1 cuenta TIME relativo del período en curso como `HISTORICAL_PROCESSES`; «este mes» sugiere vigente. Cae en lo que A6 manda a adjudicación M1-B. |
| G1b | X021 «aseo en Cali que no esté cerrado» | PV2 pide «STATE terminal»; aquí `closed` está NEGADO. Etiquetado por presencia (PV2). La tabla no dice si cuenta la polaridad. Lo mismo para TIME negado. |
| G1c | P084 «contratación directa de vigilancia» | Modalidad sin sustantivo de universo → PV6 `CURRENT_OPPORTUNITIES`, pero la contratación directa no tiene oportunidades vigentes. Una modalidad que sólo existe como histórico no debería caer en PV6. |
| G1d | cualquier tema desnudo + BUYER | Sin regla BUYER→CONTRACTS (A6): «alcaldía de X» + tema cae en PV6. No hay caso U con BUYER en este lote para no reetiquetar a mano. |

## G2. Dimensiones / operadores que el contrato no cubre

- **Valor exacto** (X057 «por 500 millones»): VALUE sólo tiene `>`, `>=`, `<`, `<=`. Etiquetado
  UNSUPPORTED «valor exacto».
- **Duración / plazo** (X055 «por 3 años»): sin dimensión; UNSUPPORTED «duración del contrato».
  Riesgo de confundirlo con `relative_period` («últimos 3 años»).
- **Referencia normativa** (X049 «ley 80 de 1993», X051 «decreto 1082 de 2015»): se etiquetó TOPIC
  contains; el año del instrumento NO es TIME. ¿Dimensión propia o UNSUPPORTED?
- **Número de contrato/proceso** (X056 «contrato 045 de 2023»): ¿PROTOCOL «ID exacto» o SEARCH +
  UNSUPPORTED? El contrato sólo cita IDs SECOP completos.
- **Semestre/trimestre** (P048): rango acotado nombrado sin fechas; etiquetado `between_dates`.
- **Pronóstico** (X087): dominio de contratación sin operación soportada; etiquetado
  `OUT_OF_DOMAIN`+DECLINE por falta de una operación «no soportada dentro del dominio».
- **Idioma** (X074 inglés): ¿se acepta? ¿TOPIC se cita en el idioma del texto o se traduce?

## G3. Conectores «y» fuera de la materia (A3 sólo regula «y» dentro del tema)

P155 «contratos en Cali y Medellín» y X078 «aseo en 2024 y en 2025»: para lugares/períodos «y» es
unión (OR), pero el contrato sólo conoce «o» como OR. Etiquetado el significado ideal OR. Un host
que lea «y» como AND devuelve vacío en silencio. Conviene una regla A3b explícita.

## G4. Homónimos geográficos: política no fijada (GEOGRAPHY axis P088–P097, X071–X073)

- Política aplicada: «en <nombre>» sin marcador de nivel = lectura dominante, CLEAR/EXECUTE
  (Córdoba, Bolívar, Sucre, Santander → departamento; Armenia → municipio del Quindío).
- «contratos de <homónimo>» (con «de», sin «en»/«alcaldía de») = AMBIGUOUS+CLARIFY (X071
  Santander, X072 Bolívar). Los casos de «de + ciudad/departamento» donde domina GEOGRAPHY se dejan
  EXECUTE+AMBIGUOUS (P108 «de Cali», X073 «del Huila»).
- El contrato da «contratos de Santander» = CLARIFY (§8) pero no la regla general. Se pide decidir:
  ¿la diferencia es «en» vs «de», o el homónimo mismo? Mientras no se fije, los Gold de homónimos son
  frágiles entre etiquetadores.
- P097 Bogotá D.C.: distrito capital ≈ municipio y ≈ departamento; `level` sin regla (se usó
  MUNICIPALITY). P097 y el resto de Bogotá en ADV usan `value_hint` «Bogotá D.C.».
- P094 «San Andrés» (gemelo P095): único AMBIGUOUS de nivel genuino (departamento vs isla/municipio).

## G5. Solapamiento de citas

- **Universo vs MODALITY** (P013, P079 «licitaciones públicas»; X081 «selecciones abreviadas»): la
  cita de universo y la de MODALITY coinciden o se solapan. El prompt cita sólo la frase de modalidad;
  ¿el universo cita el sustantivo suelto o se omite?
- **Convenio** (X082): §3 lo pone como sustantivo de universo y §4/ADR-061 como tipo composite; se
  solapan las citas.
- **`entre X y Y`** (P056, X005): dos condiciones VALUE; ¿ambas citan la frase entera o se parte
  el span? Etiquetadas con la frase entera repetida.
- **Negación**: S5 manda citar desde «no» hasta el estado («no terminados»); M4/E2 no dicen si en
  MODALITY/TOPIC/BUYER negados se cita el «no/sin/excepto». Aquí: STATE incluye el marcador, el resto no.
  Inconsistencia a resolver para medir spans de forma homogénea.

## G6. Frontera A2 (tipo vs tema) y «obra»

- P138 «prestación de servicios de vigilancia»: A2 lo manda entero a TOPIC; así pierde el filtro de
  tipo (que también activaría PV4). ¿Se conserva el tipo además del tema?
- P140 «obra» (TOPIC whole_token) frente a «obra pública» (CONTRACT_TYPE): un adjetivo cambia la
  dimensión. La frontera sólo se justifica por el listado.
- X041 «obras de licitación pública de vías»: modalidad dentro de la frase del tema. Etiquetado en
  tres condiciones (tema discontinuo). Alternativa: un solo TOPIC sin MODALITY.

## G7. Precedencia entre señales de intención y universo

- X036 «Estoy armando una propuesta para una licitación… ver contratos anteriores»: O3 literal diría
  oportunidad; el universo explícito es contratos. Etiquetado CONTRACTS RECORDS. Falta regla de
  precedencia O3 vs sustantivo de universo explícito.
- O3 con modalidad: X012 «me quiero presentar a licitaciones públicas…» etiquetado oportunidad con
  MODALITY (la intención gana a «pública»). Confirmar.

## G8. Otros

- X015 «excepto los de Bogotá y los de menos de 100 millones»: NOT(OR) vs NOT(AND).
- X047 «contratos abiertos»: estado inaplicable al universo; ¿rechazo, aclaración o traslado a procesos?
- P073 / X069: orden singular («el … de mayor valor») sin cifra; orden por valor sin dirección. Se
  etiquetaron `value_desc` sin `limit_n`.
- X089 «contratos» desnudo: ¿CLARIFY o EXECUTE (todos)? No hay umbral de condiciones mínimas.
- P128: S3 («publicados o abiertos») con `value_hint` `other:published_or_open` por no existir un valor
  canónico compuesto.
- Persistencia de eje: PK con condiciones repetidas por request usa los mismos ids `c1…` en q1 y q2
  (ids locales por request); confirmar que es la convención.
