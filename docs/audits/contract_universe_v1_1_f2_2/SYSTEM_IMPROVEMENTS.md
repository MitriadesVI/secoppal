# Mejoras observadas para el coordinador

## Incorporada en F2.2: comprobar filtro y consumo juntos

Las propiedades de workflow detectaron que `suspendidos y suspendida` y
`suscritos y suspendidos` podían tener un filtro correcto y aún dejar una
condición como objeto. Los tests nuevos exigen conjuntamente filtro efectivo,
ausencia de residuo y filas compatibles. Esto cubre el hueco del oráculo
anterior, que exceptuaba las condiciones perdidas también en el caso simple.

## Recomendada, no implementada: una evidencia de productor con filtro y spans

El productor podría devolver un único objeto inmutable con resolución,
filtro efectivo y spans consumidos. Hoy el gate comprueba la resolución y el
adaptador vuelve a solicitarla para transportar su valor. El resolvedor actual
es puro y determinista, pero una evidencia única evitaría divergencias futuras
entre lo que se acredita y lo que se consume.

Los controles temporales mostraron por qué el límite importa: consumir un tramo
que contiene fecha o marcador de actualidad interfiere con los offsets que ese
productor posee. F2.2 sólo consume como control escalar condiciones separadas por
lenguaje de unión; el contenido temporal y de objeto conserva su camino previo.
No se refactorizaron QueryRouter, Frame ni sus contratos de offsets.

## Registrada, no corregida: vocabulario aislado

Ver `CONTRACT_STATE_LEXICON_GAPS_001.md`. El guard de composiciones no sustituye
una reparación del vocabulario de estados y no la realiza implícitamente.
