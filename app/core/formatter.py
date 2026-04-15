from __future__ import annotations

from app.utils.money import format_cop


class Formatter:
    def __init__(self, max_results: int = 10):
        self.max_results = max_results

    def format_for_channel(self, results: list[dict], dataset_id: str, channel: str) -> tuple[str, list[dict]]:
        rows = self.to_rows(results, dataset_id)
        if channel == "telegram":
            return self.format_telegram(rows), rows
        if channel == "streamlit":
            return self.format_streamlit(rows), rows
        return self.format_whatsapp(rows), rows

    def to_rows(self, results: list[dict], dataset_id: str) -> list[dict]:
        rows: list[dict] = []
        for item in results[: self.max_results]:
            url = self._safe_url(item)
            if dataset_id == "p6dx-8zbt":
                rows.append(
                    {
                        "titulo": item.get("nombre_del_procedimiento", "Sin nombre"),
                        "entidad": item.get("entidad", ""),
                        "valor": format_cop(item.get("precio_base")),
                        "estado": item.get("estado_de_apertura_del_proceso", ""),
                        "fecha": item.get("fecha_de_publicacion_del", ""),
                        "url": url,
                    }
                )
            else:
                rows.append(
                    {
                        "titulo": item.get("objeto_del_contrato", "Sin nombre"),
                        "entidad": item.get("nombre_entidad", ""),
                        "valor": format_cop(item.get("valor_del_contrato")),
                        "estado": item.get("estado_contrato", ""),
                        "fecha": item.get("fecha_de_firma", ""),
                        "contratista": item.get("proveedor_adjudicado", ""),
                        "url": url,
                    }
                )
        return rows

    @staticmethod
    def _safe_url(item: dict) -> str:
        """Extract URL, handling dict format and broken login URLs."""
        url_raw = item.get("urlproceso", "")
        if isinstance(url_raw, dict):
            url_raw = url_raw.get("url", "")
        url = str(url_raw or "")
        if "Login" in url:
            ref = item.get("referencia_del_proceso") or item.get("referencia_del_contrato", "")
            if ref:
                return f"https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID={ref}"
            return ""
        return url

    def format_whatsapp(self, rows: list[dict]) -> str:
        if not rows:
            return "No encontre resultados con esos filtros. Prueba con otro departamento, entidad o rango de valor."

        lines = [f"📋 Encontre {len(rows)} resultados:\n"]
        for index, row in enumerate(rows, start=1):
            lines.append(f"*{index}.* {self._truncate(row['titulo'], 90)}")
            lines.append(f"🏛️ {row['entidad']}")
            tail = f"💰 {row['valor']}"
            if row.get("estado"):
                tail += f" | {row['estado']}"
            lines.append(tail)
            if row.get("contratista"):
                lines.append(f"🤝 {row['contratista']}")
            if row.get("url"):
                lines.append(f"🔗 {row['url']}")
            lines.append("")
        return "\n".join(lines).strip()

    def format_telegram(self, rows: list[dict]) -> str:
        if not rows:
            return "No encontre resultados con esos filtros."

        lines = [f"Encontre {len(rows)} resultados:\n"]
        for index, row in enumerate(rows, start=1):
            lines.append(f"{index}. {self._truncate(row['titulo'], 100)}")
            lines.append(f"Entidad: {row['entidad']}")
            lines.append(f"Valor: {row['valor']} | Estado: {row.get('estado', 'N/D')}")
            if row.get("contratista"):
                lines.append(f"Contratista: {row['contratista']}")
            if row.get("url"):
                lines.append(str(row["url"]))
            lines.append("")
        return "\n".join(lines).strip()

    def format_streamlit(self, rows: list[dict]) -> str:
        if not rows:
            return "No encontre resultados con esos filtros."
        return f"Encontre {len(rows)} resultados listos para explorar en tabla y tarjetas."

    @staticmethod
    def _truncate(value: str, max_length: int) -> str:
        if len(value) <= max_length:
            return value
        return value[: max_length - 3].rstrip() + "..."

