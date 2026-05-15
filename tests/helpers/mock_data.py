"""Factory helpers for building minimal SECOP-like dicts in tests."""


def make_contrato(
    entidad: str = "INVIAS",
    valor: float | None = 100_000_000,
    modalidad: str = "Licitacion Publica",
    fecha: str = "2025-01-15T00:00:00",
    objeto: str = "Contrato de prueba",
    estado: str = "Activo",
    contratista: str = "Proveedor SAS",
) -> dict:
    return {
        "nombre_entidad": entidad,
        "valor_del_contrato": valor,
        "modalidad_de_contratacion": modalidad,
        "fecha_de_firma": fecha,
        "objeto_del_contrato": objeto,
        "estado_contrato": estado,
        "proveedor_adjudicado": contratista,
        "urlproceso": "https://example.com/contract",
    }


def make_proceso(
    entidad: str = "MUNICIPIO",
    valor: float | None = 500_000_000,
    modalidad: str = "Licitacion Publica",
    fecha: str = "2025-02-01T00:00:00",
    nombre: str = "Proceso de prueba",
    estado: str = "Abierto",
) -> dict:
    return {
        "entidad": entidad,
        "precio_base": valor,
        "modalidad_de_contratacion": modalidad,
        "fecha_de_publicacion_del": fecha,
        "nombre_del_procedimiento": nombre,
        "estado_de_apertura_del_proceso": estado,
        "urlproceso": "https://example.com/process",
    }
