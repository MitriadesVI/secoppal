# Causa confirmada

Los dos tests solicitados fallaron antes de editar: NO_DOCUMENT en vez de MATCH y KeyError fecha_desde. Los mismos tests pasan en main c52d7ed. before.json y main.json conservan comandos, cwd, duración y salidas completas.

PARAMS_BEFORE_AFTER.json muestra exactamente dos claves añadidas: contract_universe=celebrated y contract_universe_version=contract-universe-1.1. Ninguna otra clave/valor cambió. explicit_document_identity rechazaba todo contenido fuera de _DOCUMENT_REPLY_KEYS: estas claves derivadas devolvían None y match_pending_document devolvía NO_DOCUMENT. El handler no reutilizaba el frame.

La reparación vive únicamente en actor_pending_document.py. Un predicado pequeño reconoce valores default de ContractUniverse y POLICY_VERSION. Sigue exigiendo METHOD_EXPLICIT_DOCUMENT y dataset no explícito; cualquier estado, error, constraint poblado, universo source_records o versión desconocida bloquea la selección. apply_request marca dataset_explicit=True para toda UniverseRequest explícita; esta es la evidencia de provenance ya disponible que hace segura la neutralidad local. No se creó un sistema general de provenance ni se eliminó el stamp.

El router devuelve params exactamente iguales antes/después de R1. La búsqueda fresca conserva universo celebrated. El handler, la política F1–F5 y los tres archivos de oráculos adjudicados permanecen intactos.

Límite: la función consume params del router. La neutralidad depende de su contrato actual de provenance (dataset_explicit y constraints); no autoriza ignorar metadata futura ni params adulterados que borren la evidencia de intención.
