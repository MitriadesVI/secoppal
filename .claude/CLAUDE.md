# Claude Code — SECOPPAL

Lee primero [`../AGENTS.md`](../AGENTS.md). Para tareas `COMPLEX`, `HIGH_RISK`
o multiagente, la autoridad compartida es
[`../docs/orquestacion/OLYMPUS_POLICY.md`](../docs/orquestacion/OLYMPUS_POLICY.md).
Este archivo sólo añade detalles del runtime Claude Code; no redefine la
política.

- Usa las herramientas disponibles en la sesión después del
  `TOOLCHAIN_READINESS`; no infieras una capacidad por archivos residuales.
- Las autorizaciones de `.claude/settings.local.json` habilitan comandos, pero
  no amplían misión, ownership, scope ni permisos destructivos.
- La lectura del código no sustituye gates ejecutados. Declara exactamente qué
  corriste y, cuando aplique, “corrí X, no la suite”.
- No ejecutes `./dev.sh`, `make dev` ni `make restart` desde un worktree.
- No uses `git stash`, `reset`, `clean` ni checkout destructivo sobre cambios
  preexistentes. No hagas merge, push, deploy o restart sin autorización.

Ruff, Semgrep, pytest e Hypothesis se aplican sólo según el cambio y el destino,
como establece `AGENTS.md`; un cambio documental usa gates estructurales.
