# Integración efectiva de Frame V1.2 — 2026-09-08

**Integrado en main por fast-forward, con Frame OFF.** El usuario aceptó el
cierre `V1_2_FRAME_GO_FOR_MERGE_OFF` y autorizó incorporar exactamente su HEAD.

- MAIN_BEFORE: `7868664757b28bed16c583ea50327d669f1501e3`.
- MAIN_AFTER_FF / candidato integrado: `32d7c39e54c10f2e97075d53f0ee0e06e7abbbad`.
- GATE_CODE_HEAD probado: `24d67fcb9195e5d79b9dca59efa38f42e0973195`.
- Runtime auditado: `4edc248e60610efdbeebb3cb8010bc7b59445cad`.
- Comando efectivo: `git merge --ff-only 32d7c39e54c10f2e97075d53f0ee0e06e7abbbad`.
- Resultado: exit 0; avance de 87 commits, sin divergencia ni commit de merge.

Se revalidaron HEAD, rama, limpieza tracked/staged, ascendencia, ausencia de
colisiones y diff-check inmediatamente antes del avance. El worktree de origen
estaba limpio y en el HEAD solicitado. La diferencia desde el SHA probado hasta
el HEAD integrado contiene únicamente 46 archivos de documentación/evidencia
del gate; no hay cambios funcionales posteriores. El inventario figura en
[integration_receipt.json](integration_receipt.json).

Se conservaron byte a byte los 244 archivos locales no versionados (6.309.886
bytes) y `.env`, comprobados mediante SHA256 antes/después. La comprobación de
colisiones incluyó rutas locales ignoradas que habría escrito el candidato.
No se publica el inventario de nombres de archivos del usuario; el recibo
conserva su huella agregada. Tracked y staged quedaron limpios tras el avance.

## Verificación posterior breve

Salidas reales conservadas junto a este informe:

```text
Ruff: All checks passed!
Semgrep: Ran 3 rules on 326 files: 10 findings.
lint-core: app/core/ limpio
Configuración: FRAME_ENABLED=False
```

Todos esos comandos finalizaron con exit 0. Semgrep terminó sin errores del
escáner y se comparó por regla/ruta/fragmento con el candidato de la segunda
corrida: 10 heredados, 0 nuevos, 0 reubicados; baseline todavía rojo. Conserva
la exclusión del archivo mayor de 1 MB declarada por el escáner. JSON completo
y [delta](semgrep_delta.json) permiten revisar la evaluación sin depender sólo
del exit code. Los logs comprimidos preservan bytes originales y sus hashes
están en [artifact_storage.json](artifact_storage.json).

Corrí Ruff, Semgrep, lint-core y la lectura aislada del flag, no la suite.
No se volvió a ejecutar pytest: la equivalencia con el código ya probado quedó
acreditada y el usuario autorizó integrar sin repetir la suite global. El
resultado previo, no una ejecución nueva, fue:

```text
13071 passed, 11 skipped, 16 xfailed in 1009.65s (0:16:49)
```

También se conservan los complementarios 47/303/115 y el corpus 89/89 del
[cierre anterior](../round2/CLOSEOUT.md), incluidos los targets de docs/audits/.
Los informes de ambas corridas permanecen intactos: su indicación histórica
«sin merge» describe el momento de aquel gate; este documento registra el
avance efectuado después de la autorización.

Ruff 0.16.4, Semgrep 1.176.0 e Hypothesis 6.165.10 disponibles. No se añadieron
propiedades para esta integración mecánica/documental. Codegraph --version y
status rechazaron Node 26.8.1; se usó Git/rg/lectura manual, sin instalar ni
suponer un índice activo. Las salidas de disponibilidad están en el recibo.

## Integrado no significa activado

El default permanece false; no había override de
`SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED` en el proceso ni en `.env`. Una lectura
fresca de configuración confirmó false, con persistencia temporal redirigida
antes de construirla y sin forzar el flag. No se cambió configuración, se
arrancó/reinició la app, se hizo push o se desplegó.

El P2 de recuperación sigue sin reparar: unlink visible, rearme fallido y fallo
de escritura de la marca después de truncate pueden admitir B anterior no
publicado y lanzar PublicationRecoveryError; la excepción no acredita bloqueo.
El reset validado fue sobre almacenamiento recuperado. No se declara
PRODUCTION_GO ni aceptación de ese riesgo para terceros.

El dogfood con Frame ON queda para un encargo posterior, en instancia y
persistencia de pruebas separadas. No se ejecutó como efecto del merge.

El commit posterior a MAIN_AFTER_FF sólo registra esta integración en evidencia,
bitácora y guía técnica; su SHA se entrega fuera del propio commit. Se resuelve
con `git log -1 --format=%H -- docs/audits/entity_profile_frame_v1_2_coordinator_gate/integration/INTEGRATION.md`.
