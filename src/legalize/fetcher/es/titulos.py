"""Explicit mapping of short titles for known norms.

When the extraction heuristic does not work, we use this mapping.
Updated manually when adding norms whose short title cannot be
automatically extracted from the official title.
"""

from __future__ import annotations

# BOE-ID → readable short title
SHORT_TITLES: dict[str, str] = {
    "BOE-A-1978-31229": "Constitución Española",
    "BOE-A-1995-25444": "Código Penal",
    "BOE-A-2015-11430": "Estatuto de los Trabajadores",
    "BOE-A-2000-323": "Ley de Enjuiciamiento Civil",
    "BOE-A-2003-23186": "Ley General Tributaria",
    "BOE-A-2015-10565": "Ley de Procedimiento Administrativo Común",
    "BOE-A-2015-11719": "Ley de Régimen Jurídico del Sector Público",
    "BOE-A-2006-7899": "Ley de Educación",
    "BOE-A-2015-11724": "Ley General de la Seguridad Social",
    "BOE-A-2018-16673": "Ley de Protección de Datos",
    "BOE-A-1996-8930": "Ley de Propiedad Intelectual",
    "BOE-A-2023-12203": "Ley de Vivienda",
}


def get_short_title(identifier: str, full_title: str) -> str:
    """Returns the short title for a norm.

    First looks in the explicit mapping. If not found, attempts
    to extract automatically from the full title.
    """
    if identifier in SHORT_TITLES:
        return SHORT_TITLES[identifier]

    return _extract_short_title(full_title)


def _extract_short_title(full_title: str) -> str:
    """Heuristic: extracts short title from the full title.

    Fallback when the norm is not in the explicit mapping.
    """
    lower = full_title.lower()

    # "Ley del/de la X" within the title
    for pattern in ["ley del ", "ley de la ", "ley de los ", "ley de las "]:
        idx = lower.rfind(pattern)
        if idx != -1:
            rest = full_title[idx + len(pattern) :].rstrip(".")
            if rest:
                return rest[0].upper() + rest[1:]

    # ", del X" / ", de la X" (last match)
    markers = [", del ", ", de la ", ", de los ", ", de las "]
    last_match = -1
    last_marker_len = 0
    for marker in markers:
        idx = lower.rfind(marker)
        if idx > last_match:
            last_match = idx
            last_marker_len = len(marker)

    if last_match != -1:
        rest = full_title[last_match + last_marker_len :].rstrip(".")
        if rest:
            return rest[0].upper() + rest[1:]

    return full_title.rstrip(".")
