# INV-SYSTEM-METADATA-DOES-NOT-CHANGE-SPEECH-ACT

Archivo workflow completo existente + tests nuevos: 61 passed (actor_after.txt).

| Superficie | Evidencia |
|---|---|
| CC, NIT, NIT con DV; match/no-match/duplicado | Tests existentes y 12 combinaciones explícitas nuevas |
| Número sin tipo | Conducta anterior cubierta por archivo existente |
| Default celebrado y versión, solos/juntos/ausentes | Cuatro subconjuntos de metadata neutral |
| Propiedad de equivalencia | Hypothesis determinista, números CC y cardinalidad pending 0/1/2 |
| Propiedad inversa | 14 constraints parametrizados, cada uno con todos los subconjuntos neutrales |
| source_records, error, versión desconocida | NO_DOCUMENT |
| Fecha, dataset explícito, estado, valor, buyer, topic, modalidad | NO_DOCUMENT |
| Seis solicitudes textuales con intención explícita | Parser real y NO_DOCUMENT |
| Año, buyer, buyer+año, valor, same-alias | Archivo workflow completo verde |
| Rank/document, orden, resolver, foreign/expired/inconclusive | Archivo workflow completo verde |
| Búsqueda fresca sin pending | Router conserva contratos y celebrated; workflow fresh existente ejecutado |

M1: 2 fallos de comportamiento. M2: 4 fallos (source_records, versión desconocida, dos estados). M3: 3 fallos de preservación de año/buyer/valor. Mutaciones sólo en memoria de subprocess, sin editar runtime en disco. Ver MUTATIONS.json y salidas completas.
