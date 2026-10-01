# A5 — Benchmark de failover de proveedor (especificación, sin elegir proveedor)

Fecha: 2026-09-30. Contrato: `M1_0_SEARCH_CONTRACT.md` §7-D1. Autor: HERA (orquestador).
Estado: especificación. No se implementa nada en Fase A; M1-G la materializa.

## 1. Qué decide este benchmark

Si un productor B puede sustituir técnicamente al productor A (hoy DeepSeek, `deepseek-chat`)
**cuando A falla por transporte**, sin cambiar el significado que el host acepta. No decide qué
modelo es «mejor intérprete»: eso es la evaluación del productor v5 sobre el Gold sellado.

Candidatos iniciales (la lista no es una elección): A = proveedor actual; B = el endpoint
OpenAI-compatible ya configurado en `.env` (`OPENCODE_*`) u otro que el usuario autorice. Ninguno
se fija por intuición: se fija por las cifras de §4.

## 2. Hallazgo previo en el código actual (entra en M1-G)

`LinguisticProducer.produce` (`app/core/foundation_producers.py`) clasifica como
`PROVIDER_ERROR` sólo las excepciones del cliente. Una respuesta **vacía**, `None`, o **truncada**
(`finish_reason == "length"`, `max_tokens=2400`) cae en `json.loads`/`decode_proposal` y sale como
`INVALID_PROPOSAL`. Bajo la regla D1 eso bloquearía el failover justo en fallos técnicos.
Taxonomía que M1-G debe implementar (y que este benchmark mide):

| Situación | Clase | ¿Failover? |
|---|---|---|
| excepción de transporte, timeout, DNS, TLS | `PROVIDER_ERROR:TRANSPORT` | sí |
| HTTP 429 / 5xx | `PROVIDER_ERROR:UNAVAILABLE` | sí |
| contenido vacío o `None` | `PROVIDER_ERROR:EMPTY` | sí |
| `finish_reason == "length"` (truncado) | `PROVIDER_ERROR:TRUNCATED` | sí |
| sin credencial / cliente ausente | `PROVIDER_ERROR:UNCONFIGURED` | sí (si B está configurado) |
| JSON completo pero mal formado / fuera de contrato | `INVALID_PROPOSAL` | **no** |
| propuesta válida que el host aclara o no soporta | `NEEDS_CLARIFICATION` / `UNSUPPORTED` | **no** |

Regla absoluta: el failover nunca es una segunda opinión. Si A respondió una propuesta completa,
esa es la interpretación del turno aunque B la hubiera dado distinta (invariante 1 del roadmap).

## 3. Diseño del benchmark

- **Mismo prompt** (SHA congelado del productor vigente), **mismo esquema**, **mismo decoder**,
  **mismo host**, **mismo Gold** (el sellado en M1-B; antes, el held-out DA-6 como cota).
- Brazos: `A`, `B`, y `A→B` con fallo inyectado (A lanza `PROVIDER_ERROR` en 100 % de los turnos
  para medir la ruta B completa a través del runtime, no sólo el cliente).
- Por turno se registran: `producer`, `provider_model_reported`, `prompt_sha256`, latencia,
  clase §2, propuesta canónica (hash del `to_dict` ordenado), `AcceptanceStatus`, razón, SoQL
  proyectado (sin ejecutar) y la etapa del ledger M0.
- Repeticiones: 2 pasadas por brazo sobre todo el Gold SEARCH + vecinos (estabilidad intra-brazo).
- Inyección de fallos en A: timeout, 429, 503, vacío, truncado → cada uno debe producir la ruta
  B y un recibo que lo diga. Inyección en ambos → mensaje D1 («no pude interpretar ahora») y
  **cero** llamadas a legacy SEARCH.

## 4. Métricas y umbrales de aceptación de B

| Métrica | Umbral para que B sea failover |
|---|---|
| `INVALID_PROPOSAL` de B | ≤ el de A + 2 pp |
| Acuerdo de IR aceptada A vs B (READY, misma proyección canónica) sobre casos READY de A | ≥ 90 % |
| Desacuerdos A/B que **cambian el SoQL** con ambos READY | 0 en polaridad, universo, ACTOR/BUYER; los demás se listan |
| Exactitud contra Gold por dimensión (B) | sin regresión > 5 pp frente a A en ninguna dimensión con n ≥ 20 |
| Vecinos secuestrados por SEARCH (B) | 0 |
| Latencia p90 de B | ≤ 8 s |
| Latencia peor caso del turno A(timeout)→B | ≤ presupuesto del turno; exige bajar el timeout de A o cobertura (hedge) — decisión M1-G con la cifra delante |
| Estabilidad intra-brazo (2 pasadas, proyección canónica) | ≥ 95 % |

Si B no cumple, D1 se aplica sin B: caída de A = «no pude interpretar ahora». No se baja ningún
umbral para que B pase.

## 5. Contrato de presentación

Una respuesta servida por B lleva recibo (`producer`, `provider_model_reported`) en la traza y en
el ledger; no se anuncia al usuario salvo que B tenga una diferencia deliberada conocida. Shadow
diferencial (M0 L4) puede comparar A/B en dogfood con A sano, sólo como instrumento.

## 6. Privacidad y coste

El corpus REAL contiene nombres y documentos de personas: sólo se envía a proveedores que el
usuario autorice como proveedor de producto. Capturas fuera de git
(`~/Documents/tools/secoppal-diagnostics/m1_search_gold/`). Antes de correr B sobre REAL se pide
autorización explícita del proveedor B.
