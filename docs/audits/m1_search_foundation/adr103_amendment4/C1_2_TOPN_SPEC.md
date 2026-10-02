# C1.2: P8 en el contrato congelado (top-N ejecutado y anunciado), v3

Rama `mission/m1-c12-ir-v2`, worktree `/Users/rodrigoortiz/Documents/secoppal-m1-c12` (HEAD `99bce8fe`: el WIP de C1.2 ya está commiteado). Autoridad:
ADR-103 enmienda 4, D9. Es un **cambio declarado al contrato C1.1** y debe entrar antes del freeze de C1.2.

**Qué cambia respecto de v2 (N6, N7):**
- los códigos se casan exactos con `match=rf"^{code}$"`;
- en el kit, sin tope ⇒ ninguno de los tres elementos; con tope ⇒ los tres;
- «nunca se aclara» queda «salvo OQ-6».

**Qué cambia respecto de v1 (por ZEUS):**
- el tipo sólo lleva un límite de representación (10¹⁸ − 1); la política «≥ 10⁹ aclara» pasa a E-ORDER;
- cada inválido se comprueba con su código;
- las versiones alternativas del TAG se derivan de la etiqueta real;
- los mutantes T2 y T5, que eran equivalentes, se sustituyen;
- la precondición de WIP queda resuelta por el usuario.

## 0. Precondición

Antes de aplicar este spec y `C1_2_FRONTIER_SPEC.md`, se commitea **tal cual** el WIP de C1.2 (decisión del usuario):
`docs/audits/m1_c1/C1_2_CANDIDATE.md` y el ajuste de `tests/test_foundation_authority_contract.py`. Ya está hecho en `99bce8fe`;
este spec parte de ese commit. Si `git status` no está limpio al empezar, STOP.

## 1. Qué está mal hoy

- `foundation_authority_values.py:33`: el comentario dice «exceeds it and is clarified, never clamped». Contradice P8
  y T3.3.
- `:523-542`: no hay sitio para la cifra pedida, y el anuncio no puede nacer del valor tipado, que es lo único que
  cruza `to_resolution`. La cota `:529` (`1 ≤ n ≤ 100`) es correcta para lo ejecutado y **se queda**.
- El insumo v2.1 #15 decía que el kit tiene un caso que espera `CLARIFY` por encima del tope. Es falso: el kit no
  tiene ningún caso de CARDINALITY por encima del tope.

## 2. `app/core/foundation_authority_values.py`

1. `:33` pasa a decir: `MAX_TOP_N = 100  # P8 (ADR-103 enm. 4): the EXECUTED n never exceeds it; a larger figure
   executes MAX_TOP_N and is announced (n_requested keeps the figure), never clarified`.
2. Constante nueva `MAX_REQUESTED_TOP_N = 10**18 - 1  # representation bound only (round-trip); the product policy on
   absurd figures lives in E-ORDER, not here`.
3. `CardinalityValue`:
   - `TAG = "cardinality@2"`. La etiqueta `@1` no tiene lector: ningún runtime importa el contrato y no hay registros
     persistidos.
   - Campos: `n`, `basis`, `n_requested`.
   - Validación en este **orden fijo**, para que cada inválido tenga un código único:

     | Paso | Comprobación | Código si falla |
     |---|---|---|
     | 1 | `_int(n, …, 1, MAX_TOP_N)` (`:529`) | `INVALID_CARDINALITY_N` |
     | 2 | `_enum(basis)` | `INVALID_CARDINALITY_BASIS` |
     | 3 | `_int(n_requested, …, 1, MAX_REQUESTED_TOP_N)` | `INVALID_CARDINALITY_N_REQUESTED` |
     | 4 | si `SINGULAR`: `n == 1` y `n_requested == 1` | `INVALID_CARDINALITY_SINGULAR` |
     | 5 | `n == min(n_requested, MAX_TOP_N)` | `INVALID_CARDINALITY_CAP` |

   - Propiedad `capped`: `n_requested > n`.
   - `encode()`: `("cardinality@2", "n=<n>", "basis=<b>", "n_requested=<k>")`.
   - `_decode`: tres campos, con control ASCII y de dígitos para las dos cifras, y `_canonical`.
4. Docstring del módulo: «CARDINALITY records the executed `n` and the requested figure; they differ only at the P8
   cap».

`foundation_authority_contract.py` no se toca.

## 3. Kit (`tests/foundation_authority_kit.py`, `_check_shape`, rama RESOLVED)

La comprobación nueva **INV-C1.2-CAP** se aplica cuando el valor es un `CardinalityValue`. Sean
A = `effect is Effect.NARROWING`, B = `"topn-cap@1" in policies` y C = `"topn.capped" in receipt.rules`:
- **con tope** (`capped`) ⇒ tienen que darse **los tres** (A, B y C);
- **sin tope** ⇒ **ninguno** de los tres (ni A, ni B, ni C);
- cualquier otra combinación, incluida una parcial, falla con `INV-C1.2-CAP`.

## 4. Tests (`tests/test_foundation_authority_contract.py`)

| Test | Cambio |
|---|---|
| `cardinality_values()` | Singular: `(1, S, 1)`. Figura: `k` sale de `one_of(integers(1, 300), integers(1, MAX_REQUESTED_TOP_N))` y el valor es `(min(k, MAX_TOP_N), F, k)`. |
| Inválidos de CARDINALITY | Salen de la lista genérica a un `parametrize` propio de pares `(build, code)`, comprobados con `pytest.raises(ContractError, match=rf"^{code}$")`, es decir, casación exacta del código. |
| `test_a_right_shaped_payload_under_another_tag_or_version_is_refused_as_such` (`:469-475`) | Las alternativas se **derivan del TAG real**: `name, version = kind.TAG.split("@")`; las etiquetas son `("other@1", f"{name}@{int(version) + 1}", f"{name}@{int(version) - 1}")`. Hoy, `TAG.replace("@1", "@2")` aplicado a `cardinality@2` devuelve la misma etiqueta y el caso falla. Si `int(version) - 1 == 0` el resultado es `@0`, que es inválido y sirve igual. |
| `test_cardinality_value_records_its_basis_and_a_singular_is_one` | Pasa a `@2`. Además: `(100,F,500).encode() == ("cardinality@2","n=100","basis=figure","n_requested=500")` y `.capped`; `(10,F,10).capped is False`; ida y vuelta de los dos; `decode(("cardinality@1",…))` falla con `match=r"^UNSUPPORTED_VALUE_TAG$"`; decodificar `n=500,n_requested=500` falla con `match=r"^INVALID_CARDINALITY_N$"`; `MAX_TOP_N == 100`; `MAX_REQUESTED_TOP_N == 10**18 - 1`. |
| `tests/test_foundation_authority_kit.py` | Tres autoridades juguete. `CapsAndDeclares` resuelve `(100,F,500)` con NARROWING, la política y la regla, y es conforme. `CapsSilently` resuelve lo mismo con EQUIVALENT y sin política: falla con `INV-C1.2-CAP`. `DeclaresWithoutCap` resuelve `(10,F,10)` con NARROWING: falla con `INV-C1.2-CAP`. `CapsWithoutRule` resuelve `(100,F,500)` con NARROWING y la política, pero sin `topn.capped`: falla con `INV-C1.2-CAP`. `RuleWithoutCap` resuelve `(10,F,10)` con EQUIVALENT pero con la regla `topn.capped`: falla con `INV-C1.2-CAP`. |

Pares `(build, code)` del `parametrize` de inválidos (F = FIGURE, S = SINGULAR):

| Constructor | Código esperado |
|---|---|
| `(0,F,0)`, `(MAX_TOP_N+1,F,MAX_TOP_N+1)`, `(True,F,1)` | `INVALID_CARDINALITY_N` |
| `(5,"figure",5)` | `INVALID_CARDINALITY_BASIS` |
| `(100,F,MAX_REQUESTED_TOP_N+1)`, `(100,F,True)`, `(100,F,None)`, `(1,F,0)` | `INVALID_CARDINALITY_N_REQUESTED` |
| `(2,S,2)`, `(MAX_TOP_N,S,MAX_TOP_N)`, `(1,S,2)` | `INVALID_CARDINALITY_SINGULAR` |
| `(100,F,99)`, `(50,F,500)`, `(10,F,11)` | `INVALID_CARDINALITY_CAP` |

## 5. Mutantes (todos pueden morir; aplicar, correr, restaurar)

| # | Mutación | La mata |
|---|---|---|
| T1 | Quitar el paso 5 (tope) | `(50,F,500)` deja de fallar |
| T2 | `MAX_TOP_N = 101` | `(MAX_TOP_N+1,F,…)` y `MAX_TOP_N == 100` |
| T3 | `encode` sin `n_requested` | pin de `encode` e ida y vuelta (Hypothesis) |
| T4 | `_decode` acepta `@1` | `match=r"^UNSUPPORTED_VALUE_TAG$"` |
| T5 | Quitar el paso 4 (singular) | `(2,S,2)` pasa a ser válido |
| T6 | `capped` pasa a ser `n_requested >= n` | `(10,F,10).capped is False` |
| T7 | Quitar o invertir INV-C1.2-CAP del kit | `CapsSilently` / `DeclaresWithoutCap` |
| T10 | El kit exige sólo uno de los tres elementos (por ejemplo, sólo A) | `CapsWithoutRule` / `RuleWithoutCap` |
| T8 | Cambiar el orden: el paso 5 antes del 4 | `(1,S,2)` da `CAP` en vez de `SINGULAR` |
| T9 | `encode` escribe `basis=figure` para un singular | ida y vuelta de `(1,S,1)` |

## 6. Gates

1. `ruff check` sobre los 4 archivos tocados.
2. Tests focales: `pytest -p no:cacheprovider tests/test_foundation_authority_contract.py
   tests/test_foundation_authority_kit.py`, más todo test que mencione `CardinalityValue` o `cardinality@`
   (`rg -l "CardinalityValue|cardinality@" tests`). Declara «corrí X, no la suite».
3. Mutantes T1–T10 muertos, con el comando y el resultado de cada uno.
4. `olympus-semgrep-delta --base <commit del WIP> --config .semgrep/secoppal-laws.yml`.
5. `rg -n "clarified, never clamped|por encima, aclarar" app docs/audits/m1_search_foundation` vacío tras aplicar
   `C0_PATCHES` (P16).
6. `test_c1_1_modules_are_not_imported_by_any_runtime_module` verde (sin cambio de runtime).

## 7. Fuera de este spec (va en E-ORDER, núcleo)

- **Autoridad de CARDINALITY:**
  - cifra entre 101 y 10⁹ − 1 ⇒ `(100, F, cifra)`, NARROWING, `topn-cap@1` y `topn.capped`;
  - **cifra ≥ 10⁹** (decisión OQ-6, política de producto que vive aquí y no en el contrato) ⇒ CLARIFY
    `UNRESOLVED_HOST_DIMENSION:CARDINALITY` con el detalle `CARDINALITY_FIGURE_OUT_OF_RANGE`.
- **Aceptación y presentación:** la aceptación registra `HostPolicy("topn-cap","1",(cond,),NARROWING)` y la
  presentación emite el aviso `TOPN_CAPPED` (INV-C0-16).
