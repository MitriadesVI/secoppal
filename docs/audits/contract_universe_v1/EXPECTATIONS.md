# Expectativas ajustadas y límites

| Caso | Expectativa anterior | Nueva regla / evidencia |
|---|---|---|
| `test_soql_builder` mantenimiento | WHERE completo <300 caracteres | El presupuesto de expansión temática excluye únicamente el predicado común; mismos dos LIKE, monto y límite |
| `test_soql_builder_phrase` | Paréntesis inmediatamente tras WHERE | El tema conserva su paréntesis después del predicado de elegibilidad |
| `test_actor_profile_scope` (23 casos × 8 aspectos) | SQL byte idéntico al snapshot previo | Se añade exclusivamente el predicado común al oráculo; identidad, filtros, SELECT, orden y transportes siguen byte idénticos; fixture histórica no se repinea |
| `test_actor_profile_render` | El encabezado iniciaba en nombre del Actor | Leyenda del universo antes del perfil; nombre y hechos conservados |
| `buyer_actor_independent_support` | Fixture válida con estado inexistente Firmado | En ejecución en esa fixture concreta; los controles incompletos/excluidos nuevos permanecen negativos |
| `test_truth_layer_contract_signature` | Aprobado era pre-firma/conflicto | Se prueba como señal posterior sin inventar fecha; enviado Proveedor ocupa los controles pre-firma |
| `test_radar_runner._make_contratos_radar` | Radar recién creado sin versión de universo | Fixture nueva estampada; control separado demuestra rechazo del Radar antiguo |
| `test_pagination_buffer` | Mutaba una constante local duplicada | Muta la autoridad común de versión; conserva el mismo reinicio esperado |
| `test_entity_profile_frame_independent_contracts` | Versión 2 desconocida | Esquema actual 2; se rechazan 1 y 3, además de versiones inválidas |
| `test_export_xlsx` libro vacío nuevo | Params sin versión | Se estampa únicamente esa fixture de búsqueda nueva; el control antiguo requiere revisión |
| `test_truth_layer_public_parity` | Ocho huellas de archivos anteriores | Cada hunk y su control se documentan en `repins.json`; ningún oráculo de salida de procesos cambió |

La prueba inicial tuvo intentos de telemetría Burr detectados por el ledger:
`red.txt` no cuenta como control rojo válido. La copia desechable de la base en
`mutations.json.integrated_base_red` reproduce los cuatro fallos por IDs, con
telemetría apagada y sin errores externos. Las mutaciones de COUNT, Frame,
exclusión después de LIMIT y pérdida de universo al recargar exigen baseline
verde y aserciones de producto; no se aceptan import errors, timeouts ni sólo
cambios de leyenda.

Se excluye del veredicto una revisión jurídica individual de expedientes y una
estimación nacional del reparto estado × fecha. La ordenación existente de
filas sin fecha y la política general de deduplicación no se rediseñaron.
Las advertencias Semgrep históricas siguen fuera del lote; no se convirtieron
en un supuesto scan limpio. No se ejecuta la suite global.
