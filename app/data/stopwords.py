"""Lenguaje funcional del dominio — la autoridad única de las palabras vacías.

``STOPWORDS`` reúne el vocabulario que NUNCA nombra un objeto contractual:
verbos conversacionales, cortesía, pronombres, comparadores, unidades y relleno.
Se extrajo de ``query_router`` porque tenía un segundo consumidor legítimo y la
alternativa era copiarlo — que es exactamente cómo nacen los dos vocabularios
divergentes del mismo concepto.

El corrector ortográfico lo consume como vocabulario **protegido por
identidad**: sin eso, ``otra`` estaba en esta lista y aun así llegaba al parser
convertida en ``obra``, un tipo contractual. La protección de una capa no sirve
de nada si otra reescribe el texto antes.

Solo datos. Sin imports de ``app.core``: cualquier capa puede leerlo.
"""
from __future__ import annotations

STOPWORDS = {
    # Conversational verbs
    "muestrame", "muestreme", "muestra", "mostrar", "mostra",
    "busca", "buscar", "buscame", "buscate", "buscando", "busco",
    "mirame", "averigua", "revisa", "cambia",
    "dame", "dime", "necesito", "quiero", "quisiera",
    "encuentra", "encontrar", "encontrame", "encontra",
    "lista", "listar", "listame",
    "consulta", "consultar", "consultame",
    "traeme", "traer", "trae",
    "decime", "digame",
    # Questions
    "cuales", "cuantos", "cuantas", "cual",
    "hay", "tiene", "tienen", "esta", "estan", "este", "esten",
    "sea", "sean",
    "encuentre", "encuentren",
    "donde", "como", "cuando",
    # Greetings (including variants with extra letters)
    "hola", "holaa", "holaaa", "buenas", "buenass", "bueno", "buenos", "ey", "oye", "ok", "okay",
    # Courtesy
    "gracias", "favor", "podrias", "puedes", "puede", "porfa",
    # Bidder intent noise
    "poder", "pueda", "puedo", "puedan",
    # Commercial bidder verbs (BIDDER-INTENT-003)
    "vender", "vendo", "vendemos", "venderle", "venderles",
    "ofrezco", "ofrecer", "ofrecemos", "ofrecen",
    # Bidder opportunity signals (not contractual topics — RATED-016)
    "oferta", "ofertas",
    # Institutional labels (not contractual topics — RATED-041)
    "oficial",
    # OR-expansion noise (not contractual topics — RATED-067)
    "similares", "similar",
    "comercializo", "comercializar", "comercializamos",
    "proveedor", "proveedores",
    # States (extracted separately)
    "abierta", "abiertas", "abierto", "abiertos",
    "adjudicado", "adjudicados",
    "celebrado", "celebrados",
    "firmado", "firmados", "firmada", "firmadas",
    "liquidado", "liquidados",
    "cerrado", "cerrados",
    # Search types
    "contrato", "contratos", "convocatoria", "convocatorias",
    "licitacion", "licitaciones", "concurso",
    "proceso", "procesos",
    "publica", "publico",
    # Articles & prepositions
    "de", "del", "en", "el", "la", "las", "los",
    "por", "para", "que", "se", "un", "una", "con", "excepto", "sin",
    "pero", "sino", "ni", "salvo",
    # Comparators
    "mayor", "mayores", "menor", "menores",
    "superior", "superiores", "supere", "superen",
    "exceda", "excedan", "rebase", "rebasen",
    "mas", "menos",
    "no", "maximo", "máximo", "minimo", "mínimo",
    # Money units
    "pesos", "millones", "millon", "smmlv", "smlmv", "smlv",
    "salario", "salarios", "minimo", "minimos",
    # Filler
    "tipo", "tema", "temas", "asunto", "asuntos", "materia", "sector",
    "area", "área", "linea", "línea", "relacionado", "relacionados",
    "vigente", "vigentes", "sobre", "todos", "todas", "todo", "toda",
    # Temporal adverbs (not contractual objects)
    "actualmente", "ahora", "hoy", "recientemente", "momento",
    # Temporal week/day (not contractual objects)
    "semana", "dia", "dias", "día", "días", "ayer",
    "mes", "meses",
    # Interrogative verbs ("qué ha hecho/hizo/gastó X") — no son objetos
    "hecho", "hizo", "hace", "hacen", "haciendo", "gastado", "tenido",
    # Indefinite pronouns
    "algun", "alguna", "algunos", "algunas", "alguno", "algunos", "ningun", "ninguna",
    "otro", "otra", "otros", "otras",
    # Demonstrative pronouns / anaphoric refinement words.
    # Aparecen en follow-ups tipo "muestrame solo aquellos que sean mas de X".
    # Sin scrub, contaminan 'objeto' y borran el topic heredado.
    "solo", "solos", "sola", "solas", "solamente", "unicamente", "únicamente",
    # Follow-up refinement verbs — "filtra por X", "aplicá filtro", "ponelo entre X y Y"
    "filtra", "filtrar", "filtrame", "filtrarme", "filtrado", "filtrados",
    "aplica", "aplicar", "aplicale", "aplicame",
    "pon", "pone", "poner", "ponelo", "ponle", "poneme",
    "deja", "dejar", "dejame",
    "limita", "limitar", "limitame", "limitalo",
    "acota", "acotar", "acotame", "acotalo",
    "restringe", "restringir", "restrinjame",
    "aquel", "aquella", "aquellos", "aquellas",
    "eso", "esos", "esa", "esas",
    "esto", "estos", "estas",  # nota: "esta"/"estan"/"este"/"esten" ya están arriba
    "mismo", "misma", "mismos", "mismas",
    # Value/size adjectives (ordering instructions, not objects)
    "caro", "caros", "cara", "caras",
    "costoso", "costosos", "costosa", "costosas",
    "grande", "grandes", "importante", "importantes",
    "barato", "baratos", "barata", "baratas",
    "reciente", "recientes", "ultimo", "ultimos", "ultima", "ultimas",
    "nuevo", "nuevos", "nueva", "nuevas",
    "mejor", "mejores", "peor", "peores",
    "alto", "altos", "alta", "altas",
    "bajo", "bajos", "baja", "bajas",
    "valor", "cuantia", "cuantía",
    # Months (not contractual objects)
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
    # More temporal
    "semestre", "trimestre", "bimestre", "periodo", "anual", "mensual",
    "ano", "año",
    "pasado", "pasada", "anterior", "siguiente", "proximo",
    # Semantic-layer scrubbed terms (should already be consumed, safety net)
    "departamento", "departamentos",
    "oportunidades", "oportunidad",
    "publicados", "publicadas",
    "presentar", "presentarme", "presentarse",
    "licitar",
    "postular", "postularme", "postularse",
    "aplicar", "aplicarme", "aplicarse",
    "participar", "ofertar", "competir", "concursar",
    "pueda", "puedas", "puedan", "puedo",
    "tenga", "tengan", "tiene", "tienen",  # "que tiene la alcaldia..."
    "provincia",  # sometimes used as synonym for departamento
    # Conversational filler confirmed by feedback audit (1.2.a)
    "sido", "han", "entre",  # rango "entre X y Y"
    "convocadas", "convocado", "convocados",
    "disponibles", "disponible",
    "ver", "mirar", "revisar",
    "conocer", "saber", "identificar",
    "existe", "existen",
    "ofrecen", "ofrece",
    # CORPUS-AN003: verbos/cuantificadores analíticos. No son objeto
    # contractual; son señal de pregunta agregada ("cuánto suma/n", "cuántos
    # contratos hay", "total contratado"). El detector analítico vive en
    # analytics.py; aquí solo se previene que contaminen 'objeto'.
    "cuanto", "cuanta",
    "suma", "suman", "sumaron", "sumo", "sumó",
    "total", "totales",
    # Sprint 5 — VALUE-POLARITY residual scrub:
    # "por debajo de X" → polarity already sets valor_max, but "debajo"
    # leaks into objeto when "por" is consumed as stopword earlier.
    "debajo",
    # PARSER-POLARITY-MONETARY-002: post-number polarity tokens
    "adelante", "arriba", "abajo",
}
