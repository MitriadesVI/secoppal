# SECOPPAL Radar — notas iniciales de producto

## Frase clave

SECOPPAL Search encuentra.  
SECOPPAL Advisor interpreta.  
SECOPPAL Radar vigila.

## 1. Qué sería “Radar” en SECOPPAL

Un Radar es una búsqueda guardada + seguimiento periódico + lectura asesora.

Ejemplos:

- Radar Adulto Mayor Barranquilla
- Radar PAE Atlántico
- Radar Cultura Cesar
- Radar Parques y Canchas Colombia
- Radar Fundación 2030
- Radar Contratos Modificados Adulto Mayor

Cada Radar tendría:

1. Tema
2. Entidad / territorio / contratista
3. Dataset: procesos, contratos o ambos
4. Filtros: año, estado, valor, modalidad
5. Frecuencia: diaria, semanal, manual
6. Tipo de alerta: nuevo, cambio, valor alto, contratista repetido, estado cambiado
7. Resumen asesor

## 2. Qué valor entrega

### Radar de oportunidades

Para alguien que quiere licitar:

- Avísame oportunidades abiertas de PAE en Atlántico.
- Avísame procesos abiertos de construcción de parques o canchas.
- Avísame convocatorias de cultura superiores a 500 millones.

Respuesta ideal:

```text
🔔 Radar PAE Atlántico

Encontré 2 oportunidades nuevas.

1. Operación PAE — Municipio X
   Valor: $...
   Estado: Abierto
   Lectura: parece operación integral, revisar capacidad financiera y experiencia.

2. Dotación de menaje escolar — Municipio Y
   Valor: $...
   Lectura: no es operación PAE, pero puede interesar a proveedores de dotación.

Opciones:
1. Ver detalles.
2. Seguir este proceso.
3. Descargar documentos.
4. Comparar con procesos similares.
```

### Radar de supervisión

Para seguimiento de adulto mayor:

- Sigue contratos de adulto mayor en Barranquilla.
- Avísame si aparece un contrato nuevo, modificado, suspendido o terminado.
- Avísame si FUNDACARIBE, FUNSOCOM o Fundación 2030 aparecen en nuevos contratos.

Respuesta ideal:

```text
🔔 Radar Adulto Mayor Barranquilla

Cambios detectados:
- 1 contrato nuevo.
- 2 contratos modificados.
- 1 operador repetido frente a vigencia anterior.

Lectura:
El nuevo contrato parece asociado a atención integral de adultos mayores. El valor está dentro del rango observado para contratos CDB, pero conviene compararlo con 2025.
```

### Radar de control / veeduría

- Avísame contratos mayores a $1.000 millones en cultura del Cesar.
- Avísame si una entidad publica muchos contratos similares.
- Avísame si aparecen valores atípicos.

Este radar tiene enfoque de riesgo.

## 3. Capas del producto Radar

### Nivel 1 — Búsqueda guardada

MVP simple:

- guardar esta búsqueda
- mis radares
- revisar radar

Ejemplo:

```text
Usuario:
contratos de adulto mayor de alcaldia de barranquilla de 2026

Usuario:
guardar como Radar Adulto Mayor 2026
```

SECOPPAL guarda el QueryFrame.

### Nivel 2 — Detección de novedades

Cada radar compara:

- última corrida
- vs
- corrida actual

Detecta:

- resultados nuevos
- resultados que ya estaban
- resultados que desaparecieron
- cambios de estado
- cambios de valor
- nuevos contratistas

### Nivel 3 — Digest asesor

No mandar 50 filas. Mandar lectura:

```text
Desde la última revisión encontré 4 novedades:
1. 2 procesos nuevos.
2. 1 contrato modificado.
3. 1 nuevo contratista.
4. Ningún valor atípico.
```

### Nivel 4 — Radar inteligente

Cuando ya exista ranking:

- separa coincidencia directa / probable
- descarta ruido
- prioriza oportunidades accionables
- marca anomalías
- sugiere próximos pasos

## 4. Arquitectura técnica simple

No requiere Redis al comienzo. Se puede empezar con SQLite/Postgres.

### Tabla `radars`

- id
- name
- description
- query_frame_json
- dataset
- frequency
- channel
- enabled
- created_at
- last_run_at
- owner

Ejemplo de `query_frame_json`:

```json
{
  "dataset": "procesos",
  "dataset_explicit": true,
  "scope": {
    "departamento_resolved": "ATLÁNTICO"
  },
  "topic": ["alimentacion_escolar"],
  "modifiers": {
    "estado": "Publicado"
  },
  "intent_type": "new_search"
}
```

### Tabla `radar_runs`

- id
- radar_id
- started_at
- finished_at
- status
- total_count
- new_count
- changed_count
- error

### Tabla `radar_seen_results`

- radar_id
- result_key
- urlproceso
- referencia
- title
- entity
- value
- status
- first_seen_at
- last_seen_at
- snapshot_hash
- last_snapshot_json

La clave `result_key` puede ser:

- urlproceso
- referencia_del_proceso
- id_del_proceso
- referencia_del_contrato

## 5. Flujo del Radar

1. Cargar radars activos.
2. Reconstruir params desde QueryFrame.
3. Construir SoQL.
4. Consultar SECOP.
5. Deduplicar resultados.
6. Rankear relevancia.
7. Comparar contra `radar_seen_results`.
8. Detectar novedades/cambios.
9. Generar resumen asesor.
10. Notificar o guardar digest.

## 6. Tipos de alertas

Arranque recomendado:

1. Nuevo proceso abierto.
2. Nuevo contrato firmado.
3. Cambio de estado.
4. Valor alto.
5. Contratista nuevo.
6. Contratista repetido.
7. Resultado probable, pero requiere revisión.
8. Posible dato anómalo.

Después:

9. Adenda/documento nuevo.
10. Fecha de cierre próxima.
11. Cambio de presupuesto.
12. Contrato derivado de proceso.

## 7. Interfaz

En Streamlit podría existir una pestaña:

- 🔎 Buscar
- 📡 Radares
- ⭐ Guardados
- ⚠️ Alertas

En Radares:

```text
Radar Adulto Mayor Barranquilla
Estado: activo
Última revisión: hoy 8:00 a.m.
Nuevos: 2
Cambios: 1
Acciones:
- Revisar novedades
- Pausar
- Editar filtros
- Ejecutar ahora
```

## 8. Casos de radar para el dominio actual

### Radar Adulto Mayor Barranquilla

- dataset: contratos
- entidad: Distrito de Barranquilla
- topic: adulto_mayor

Alertas:

- contratos nuevos
- modificaciones
- operadores repetidos
- valores altos

### Radar Centros de Bienestar

Topic:

- adulto_mayor
- atención integral
- vulnerabilidad
- abandono
- asilo
- hogar
- centro de bienestar

Contratistas watchlist:

- Asilo San Antonio
- Hogar Granja San José
- Hogar San Camilo
- Ana Delia
- Tus Abuelos

### Radar PAE Caribe

- dataset: procesos
- topic: alimentacion_escolar
- departamentos:
  - Atlántico
  - Bolívar
  - Magdalena
  - Cesar

Alertas:

- oportunidades abiertas
- valores altos
- operación integral
- menaje/dotación

### Radar Cultura Cesar

- entidad: Gobernación del Cesar
- topic: cultura

Alertas:

- procesos nuevos
- contratos altos
- festivales/eventos
- infraestructura cultural

## 9. Relación con el buscador actual

El buscador debe producir el radar.

Flujo ideal:

```text
Usuario:
que oportunidades hay para construccion de canchas o parques

SECOPPAL:
Encontré 652 procesos. Algunos son construcción directa, otros interventorías o suministros.

Sugerencias:
1. Ver solo procesos abiertos.
2. Quitar servicios profesionales.
3. Guardar como radar.
4. Ordenar por mayor valor.
```

Cuando el usuario elige `3`:

```text
SECOPPAL pregunta:
¿Cómo quieres llamarlo?

1. Radar Canchas y Parques
2. Radar Construcción deportiva
3. Escribir otro nombre
```

## 10. Dependencias antes de Radar completo

No hacer Radar completo hasta cerrar estas bases:

1. QueryFrame estable.
2. Guard anti-WHERE 1=1.
3. Diccionario de dominio inicial.
4. Deduplicación.
5. Ranking de relevancia básico.
6. Response policy asesora.

Sí se puede empezar a diseñar ya el modelo de datos.

## 11. MVP Radar factible

Primera versión realista:

- guardar búsqueda
- listar radares
- ejecutar radar manualmente
- detectar nuevos resultados
- mostrar digest

Sin cron, sin Redis, sin Telegram todavía.

Comandos:

```text
guardar esta búsqueda como Radar PAE Atlántico
revisar mis radares
ejecutar Radar PAE Atlántico
```

Eso ya sería útil.

## 12. Versión 2

- ejecución diaria automática
- notificación por Telegram/email
- cambios de estado
- seguimiento de proceso individual
- documentos nuevos

## 13. Versión 3

- análisis de pliegos/documentos
- alertas por requisitos habilitantes
- comparación con históricos
- scoring de oportunidad
- scoring de riesgo

## 14. Radar de actores contractuales

El Radar no solo debe vigilar temas; también debe vigilar actores.

Actores incluidos:

- proveedores / contratistas
- uniones temporales
- consorcios
- representantes
- miembros de estructuras plurales
- operadores recurrentes
- nombres similares o variantes

### Por qué incluirlo

SECOPPAL no solo debe responder:

```text
¿Qué contrató una entidad?
```

También debe responder:

```text
¿Quién está apareciendo?
¿Con quién se repite?
¿En qué entidades?
¿Con qué valores?
¿Bajo qué figuras?
¿Como persona jurídica sola, consorcio o unión temporal?
```

Esto es valioso para:

- supervisión
- control ciudadano
- análisis de concentración
- inteligencia de mercado
- seguimiento de operadores
- preparación para licitar

### Ejemplos de uso

```text
muéstrame contratos de FUNDACARIBE
sigue a Fundación 2030 en contratos de adulto mayor
qué consorcios han contratado PAE en Atlántico
uniones temporales relacionadas con infraestructura educativa en 2026
proveedores que aparecen en cultura Cesar
contratistas repetidos en adulto mayor Barranquilla 2025 vs 2026
```

### Funcionalidad mínima

Primera versión:

1. Buscar proveedor/contratista por nombre.
2. Ver contratos/procesos asociados.
3. Agrupar por entidad contratante.
4. Agrupar por año.
5. Mostrar valores acumulados.
6. Guardar como radar.

Ejemplo:

```text
Radar FUNDACARIBE
- Nuevos contratos
- Cambios de estado
- Nuevas entidades contratantes
- Contratos similares por objeto
- Aparición en consorcios o UT, cuando sea detectable
```

### Dificultad real

Aquí el reto no es buscar. El reto es normalizar identidad.

Ejemplo:

- FUNDACARIBE
- Fundación Caribe
- FUNDACION CARIBE
- Fundacaribe
- FUNDACARIBE NIT...

Y con estructuras plurales:

- UNIÓN TEMPORAL X
- UT X
- CONSORCIO X
- Consorcio Alimentación 2026

A veces SECOP solo muestra el nombre de la unión temporal/consorcio, no siempre sus integrantes.
Entonces habría tres niveles:

Nivel 1:
Buscar por nombre visible en SECOP.

Nivel 2:
Normalizar variantes del mismo proveedor.

Nivel 3:
Relacionar miembros de consorcios/UT cuando haya documentos o datos disponibles.

### Roadmap sugerido

Prioridad mediana, pero estratégica:

- Radar v1: búsquedas guardadas por tema/entidad
- Radar v1.1: proveedores/contratistas
- Radar v1.2: alertas por proveedor
- Radar v1.3: consorcios/uniones temporales
- Radar v2: red de actores contractuales

### Modelo de datos futuro

#### `actors`

- actor_id
- canonical_name
- actor_type: proveedor | union_temporal | consorcio | entidad | persona_natural
- normalized_names
- nit/documento

#### `actor_aliases`

- actor_id
- alias
- source
- confidence

#### `actor_contract_links`

- actor_id
- contract_id/process_id
- role: proveedor | integrante | representante | entidad_contratante
- value
- date
- source

### Etiqueta sugerida

SECOPPAL Radar — Actores contractuales

### Descripción

Seguimiento y análisis de proveedores, contratistas, uniones temporales y consorcios,
con normalización de nombres, detección de apariciones nuevas, concentración por
entidad/tema/año y alertas por cambios o nuevos contratos.

## Recomendación

Mientras se fortalece el buscador, dejar Radar diseñado así:

Radar = QueryFrame guardado + job de revisión + diff + digest asesor.

Primer MVP recomendado después del buscador:

Guardar búsqueda + ejecutar seguimiento manual + detectar nuevos resultados.

Prioridad mediana futura:

Radar de actores contractuales — proveedores, contratistas, consorcios y uniones temporales.
