"""
Departamento mappings for SECOP II.
Values verified from discover_secop.py output — April 2026.

CRITICAL: SECOP uses DIFFERENT department names per dataset!
  - Procesos: departamento_entidad = "Distrito Capital de Bogotá", "Atlántico", etc.
  - Contratos: departamento = same values

The values have tildes and specific capitalization. "ATLANTICO" does NOT work.
"Atlántico" is the correct value.
"""

# ---------------------------------------------------------------------------
# Alias → SECOP official value
# Keys are normalized (lowercase, no tildes) for matching
# Values are the EXACT strings SECOP expects (with tildes!)
# ---------------------------------------------------------------------------

DEPARTAMENTOS: dict[str, str] = {
    # Amazonas
    "amazonas": "Amazonas",
    "en amazonas": "Amazonas",
    "del amazonas": "Amazonas",
    # Antioquia
    "antioquia": "Antioquia",
    "en antioquia": "Antioquia",
    "del antioquia": "Antioquia",
    "de antioquia": "Antioquia",
    # Arauca
    "arauca": "Arauca",
    "en arauca": "Arauca",
    "del arauca": "Arauca",
    # Atlántico
    "atlantico": "Atlántico",
    "en atlantico": "Atlántico",
    "del atlantico": "Atlántico",
    "de atlantico": "Atlántico",
    "depto del atlantico": "Atlántico",
    "departamento del atlantico": "Atlántico",
    "departamento de atlantico": "Atlántico",
    # Bogotá
    "bogota": "Distrito Capital de Bogotá",
    "en bogota": "Distrito Capital de Bogotá",
    "de bogota": "Distrito Capital de Bogotá",
    "del bogota": "Distrito Capital de Bogotá",
    "distrito capital": "Distrito Capital de Bogotá",
    "distrito capital de bogota": "Distrito Capital de Bogotá",
    "la capital": "Distrito Capital de Bogotá",
    "bogota dc": "Distrito Capital de Bogotá",
    "bogota d.c.": "Distrito Capital de Bogotá",
    # Bolívar
    "bolivar": "Bolívar",
    "en bolivar": "Bolívar",
    "del bolivar": "Bolívar",
    "de bolivar": "Bolívar",
    # Boyacá
    "boyaca": "Boyacá",
    "en boyaca": "Boyacá",
    "del boyaca": "Boyacá",
    "de boyaca": "Boyacá",
    # Caldas
    "caldas": "Caldas",
    "en caldas": "Caldas",
    "del caldas": "Caldas",
    # Caquetá
    "caqueta": "Caquetá",
    "en caqueta": "Caquetá",
    "del caqueta": "Caquetá",
    # Casanare
    "casanare": "Casanare",
    "en casanare": "Casanare",
    "del casanare": "Casanare",
    # Cauca
    "cauca": "Cauca",
    "en cauca": "Cauca",
    "del cauca": "Cauca",
    "de cauca": "Cauca",
    # Cesar
    "cesar": "Cesar",
    "en cesar": "Cesar",
    "del cesar": "Cesar",
    "de cesar": "Cesar",
    # Chocó
    "choco": "Chocó",
    "en choco": "Chocó",
    "del choco": "Chocó",
    # Córdoba
    "cordoba": "Córdoba",
    "en cordoba": "Córdoba",
    "del cordoba": "Córdoba",
    "de cordoba": "Córdoba",
    # Cundinamarca
    "cundinamarca": "Cundinamarca",
    "en cundinamarca": "Cundinamarca",
    "del cundinamarca": "Cundinamarca",
    "de cundinamarca": "Cundinamarca",
    "cundi": "Cundinamarca",
    # Guainía
    "guainia": "Guainía",
    "en guainia": "Guainía",
    # Guaviare
    "guaviare": "Guaviare",
    "en guaviare": "Guaviare",
    # Huila
    "huila": "Huila",
    "en huila": "Huila",
    "del huila": "Huila",
    # La Guajira
    "guajira": "La Guajira",
    "la guajira": "La Guajira",
    "en la guajira": "La Guajira",
    "en guajira": "La Guajira",
    # Magdalena
    "magdalena": "Magdalena",
    "en magdalena": "Magdalena",
    "del magdalena": "Magdalena",
    # Meta
    "meta": "Meta",
    "en meta": "Meta",
    "del meta": "Meta",
    "en el meta": "Meta",
    # Nariño
    "narino": "Nariño",
    "en narino": "Nariño",
    "del narino": "Nariño",
    "de narino": "Nariño",
    # Norte de Santander
    "norte de santander": "Norte de Santander",
    "en norte de santander": "Norte de Santander",
    "norte santander": "Norte de Santander",
    "cucuta": "Norte de Santander",  # capital, common reference
    # Putumayo
    "putumayo": "Putumayo",
    "en putumayo": "Putumayo",
    "del putumayo": "Putumayo",
    # Quindío
    "quindio": "Quindío",
    "en quindio": "Quindío",
    "del quindio": "Quindío",
    # Risaralda
    "risaralda": "Risaralda",
    "en risaralda": "Risaralda",
    "del risaralda": "Risaralda",
    # San Andrés
    "san andres": "San Andrés, Providencia y Santa Catalina",
    "san andres y providencia": "San Andrés, Providencia y Santa Catalina",
    "san andres providencia": "San Andrés, Providencia y Santa Catalina",
    "en san andres": "San Andrés, Providencia y Santa Catalina",
    # Santander
    "santander": "Santander",
    "en santander": "Santander",
    "del santander": "Santander",
    "de santander": "Santander",
    # Sucre
    "sucre": "Sucre",
    "en sucre": "Sucre",
    "del sucre": "Sucre",
    # Tolima
    "tolima": "Tolima",
    "en tolima": "Tolima",
    "del tolima": "Tolima",
    # Valle del Cauca
    "valle": "Valle del Cauca",
    "valle del cauca": "Valle del Cauca",
    "en valle": "Valle del Cauca",
    "en el valle": "Valle del Cauca",
    "del valle": "Valle del Cauca",
    "en valle del cauca": "Valle del Cauca",
    # Vaupés
    "vaupes": "Vaupés",
    "en vaupes": "Vaupés",
    # Vichada
    "vichada": "Vichada",
    "en vichada": "Vichada",
}

# Official SECOP values (for fuzzy matching fallback)
DEPARTAMENTOS_OFICIALES: list[str] = sorted(set(DEPARTAMENTOS.values()))
