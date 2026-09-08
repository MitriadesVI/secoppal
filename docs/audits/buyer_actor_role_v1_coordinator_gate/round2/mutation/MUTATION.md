# Controles por mutación (round 2)

Dos mutaciones, ambas en la copia desechable `base-runtime`, nunca en el candidato.

## 1. El test H4 adjudicado es una regresión efectiva

El caso `test_new_topic_should_clear_inherited_ordering_signal`, ya sin el marcador
xfail, se ejecutó contra el runtime BASE `52a6bcc` restituido byte a byte en `app/`:

```
FAILED tests/test_h4_fallback.py::test_new_topic_should_clear_inherited_ordering_signal
E   AssertionError: assert 'adulto_mayor' not in ((['adulto_mayor']))
    where {'ciudad': 'Cali', 'dataset': 'contratos', 'limit': 50, 'objeto': ['adulto_mayor'], ...}
1 failed in 1.19s
```

Falla en la aserción de contaminación de contexto, no sólo en `ordering_signal`.
El test discrimina BASE de candidato: no es una expectativa ornamental.

## 2. Retirar el aislamiento del opt-in vuelve a detectarse

Sobre una copia del CANDIDATO se quitaron exclusivamente las dos líneas que atan
`with_timeout` / `derived_for_timeout` a la fuente sintética:

```
4 passed, 4 errors in 1.32s
ERROR tests/test_buyer_actor_role_local_v1.py::test_local_index_read_only[cartagena-False]
ERROR ... [cartagena-True]  ERROR ... [barranquilla-False]  ERROR ... [barranquilla-True]
WARNING app.core.observer:observer.py:225 aggregate value_stats failed: GATE_UNAUTHORIZED_NETWORK ...
```

Los **cuerpos siguen pasando**: el observer absorbe la excepción del agregado
(`except Exception: logger.warning`). Lo que falla es el registro externo
comprobado DESPUÉS del producto. El mecanismo de detección está vivo, y el verde
del candidato no proviene de haberlo desactivado.

## 3. Los agregados llegan de verdad a la fuente sintética

`aggregates_reach_synthetic_source.json`: con el aislamiento puesto, cada caso pasa
de 1 a **6 llamadas evaluadas en SQLite** (la principal más los cinco agregados del
observer), con `violations: null`. Los cinco agregados conservan el comprador y el
monto — la guarda estricta del fixture ahora cubre cinco consultas más, no menos:

```
SELECT nombre_entidad, count(*) AS cnt WHERE unaccent(UPPER(nombre_entidad)) LIKE
unaccent(UPPER('%DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA%'))
AND valor_del_contrato >= 2000000000 GROUP BY nombre_entidad ORDER BY cnt DESC LIMIT 5
```
