# V1.2-B — Cierre

**Veredicto:** `V1_2_B_READY_FOR_INDEPENDENT_REVIEW`.

No se emite `PRODUCTION_GO`. No hay merge, no hay push y la activación
productiva sigue **apagada por defecto**. La revisión independiente y el gate
global del orquestador son posteriores.

## 1. SHAs

| | |
|---|---|
| Base congelada | `4c86d5bafaa3296b73c6523465b33efa4311910a` |
| HEAD de código probado de R3d (ancestro verificado) | `e4b85db855d50f7e5671a515bfe1b26c882c2867` |
| HEAD probado de esta entrega | `11399dafc8a61e98db3d4fc5fbc637c8e0b4ca46` |
| HEAD final | se registra en la respuesta del carril: el commit de evidencia no puede contener su propio SHA sin autorreferencia |
| Rama | `feature/entity-profile-frame-v1-2-b-integration` |
| Worktree | `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b-integration` |

Commits del intervalo:

```text
11399da test(entity-profile): kill eleven behavioural mutations and honour the injected clock
2990fe1 test(entity-profile): cover the mandatory conversational semantics and B invariants
20aea78 test(entity-profile): turn the eight B scenarios into real integration tests
21fd633 feat(workflow): admit continuation before the dispatcher and publish the turn
d499d5d feat(entity-profile): add durable state, evidence and publication primitives
```

(más el commit de esta evidencia y de la puerta de utilidad).

## 2. Inventario final

Módulos nuevos en `app/` (13):

```text
app/core/conversation_envelope.py          sobre versionado del turno
app/core/store_lock.py                     exclusión por chat entre procesos
app/core/entity_profile/profile_state.py   Frame confirmado + parche pendiente
app/core/entity_profile/patch_codec.py     serialización tipada del parche
app/core/entity_profile/pending_resolution.py  desenlace de una pendiente
app/core/entity_profile/frame_alternatives.py  alternativas con operaciones tipadas
app/core/entity_profile/frame_runner.py    ejecuta el SQL congelado del plan
app/core/entity_profile/frame_sections.py  Plan + Evidence -> secciones
app/core/entity_profile/frame_response.py  Plan + Evidence -> respuesta
app/core/entity_profile/execution_record.py  snapshot publicable
app/core/entity_profile/frame_turn.py      el turno completo
app/core/entity_profile/focus_labels.py    cómo se nombra un foco a una persona
app/core/workflow/continuation.py          ContinuationContract
app/core/workflow/admission_rules.py       el clasificador puro
app/core/workflow/admission.py             el grafo Burr de admisión
app/core/workflow/profile_route.py         perfil -> contrato de resultado
app/core/workflow/turn_publication.py      publicar el registro completo
```

Modificados (7):

```text
app/config.py                              flag SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED (default False)
app/core/conversation_store.py             reserve_turn / publish_turn / load_envelope
app/core/orchestrator.py                   admisión antes de _dispatch + publicación
app/core/entity_profile/turn.py            helpers públicos + alias de compatibilidad
app/core/entity_profile/render.py          profile_fact_lines (era privado)
app/core/entity_profile/frame_render.py    event_verb (era privado)
app/core/entity_profile/frame_compiler.py  aclaración en idioma de producto
app/core/entity_profile/claim_adapters.py  asks_to_clear_period + consumo de sus términos
streamlit_app.py                           el chip transporta la referencia publicada
```

Tests nuevos (7 archivos, 196 pruebas) y un runner de mutaciones en
`scripts/audits/entity_profile_frame_v1_2_b/`.

## 3. Dos defectos reales encontrados al integrar

1. **`sin filtro de fecha` inventaba un tema negativo.** El «sin» lo lee el
   router como negación y publica `objeto_exclude=['filtro','fecha']`. Sin
   consumir esos términos, pedir quitar la fecha AÑADÍA un filtro temático que
   nadie pidió y que llegaba al SQL. La autoridad de limpieza ahora consume su
   propio vocabulario, con la misma disciplina de R3d.
2. **El reloj inyectado no gobernaba la ruta.** `today` llegaba a
   `run_profile_turn` y no bajaba a `collect_claims`; un periodo relativo se
   resolvía con la fecha del sistema. Lo encontró la mutación M11, no una
   lectura del código.

## 4. Límites declarados

- **Alternativas sólo para la aclaración de capacidad.** Las cuestiones que
  bloquean en la composición de claims —`SOURCE_CHOICE_AMBIGUOUS`,
  `AMBIGUOUS_PERIOD_ANCHOR`, `UNSUPPORTED_EVENT`— publican la pregunta sin
  alternativas preconstruidas, y la persona reformula. No convierte ningún caso
  positivo en aclaración: la familia R5, que es la que el diseño exige poder
  completar por selección, sí las publica y las aplica.
- **Un turno que no es del perfil limpia el estado del perfil.** Es una
  decisión deliberada: si el resultado visible es otro, conservar el Frame
  confirmado haría que un «proveedores» posterior profundizara en algo que ya
  no está en pantalla. El diseño no lo especifica en ese sentido; se declara
  para que la revisión pueda objetarlo.
- **`flock` es la exclusión entre procesos.** En una plataforma sin `fcntl`
  queda sólo el candado en memoria; el código lo dice explícitamente en vez de
  fingir la garantía. Una instalación con escritores en almacenes que no
  comparten sistema de archivos necesita otra garantía, como advierte el diseño.
- **La carrera de publicación no se pudo forzar de forma determinista.** La
  comprobación de `expected_revision` bajo la exclusión cierra casi toda la
  ventana por sí sola; la mutación que quita `flock` sobrevivió a la prueba de
  cuatro publicadores y sólo muere contra una prueba del mecanismo (dos
  procesos no sostienen el mismo candado). Se declara en vez de presentarlo
  como una carrera reproducida.
- **No se promete exactly-once de red ni recepción física por el canal.** El
  contrato describe la respuesta publicada por el servidor.
- **Índice de Codegraph menor que el de la auditoría** (18.878 aristas frente a
  49.751). Se usó `impact` y se completó con `rg` y lectura manual; se declara
  el límite en vez de presentarlo como cobertura equivalente.
- **La matriz de aceptación se redactó junto con las pruebas**, no como
  artefacto previo separado. `INVENTORY.md` sí se escribió antes de editar.
- **Los adversariales congelados del reaudit no están en esta línea**: A2 los
  portó desde el árbol de auditoría `e024753` y B conserva esos ports intactos.
  Ver `ADVERSARIAL_MATRIX.md` §1.

## 5. Encontrado fuera del carril y no tocado

- Los diez hallazgos de Semgrep del baseline global.
- `ENTITY_ANALYSIS_V1.md` sigue afirmando que los siete P1 están cerrados; la
  revisión independiente prevalece, y ese documento no es de este carril.
- El bare «más» no es una frase de paginación para `is_pagination_phrase`, así
  que en la ruta tabular sigue cayendo al parser. B lo reconoce en su propia
  admisión —donde importa— y no cambia esa función compartida.
- `docs/decisions_log.md` y `docs/decisions/` **no se tocaron**: B no ocupa un
  número de ADR. La actualización canónica de la bitácora queda pendiente para
  el orquestador, con el número que salga del máximo global.
- `docs/orquestacion/CARRILES.md` no describe este carril; su actualización es
  de la zona compartida y no se hizo.

## 6. Qué NO se ejecutó

- **La suite completa: NO EJECUTADA.** Corrí el target de R3d ampliado (1.887),
  los consumidores adyacentes (472 + 2 xfail preexistentes), el corpus, la
  puerta de utilidad, las propiedades y la campaña de mutaciones. **Corrí X, no
  la suite.** Queda para el orquestador, en serie, sobre el candidato final.
- No se levantó la app, no se usó red, no se llamó a SECOP ni al LLM.
- No se crearon radares, no se enviaron mensajes y no se escribió en `data/`.
- No se ejecutó ninguna prueba live ni ninguna probe de fuente.
