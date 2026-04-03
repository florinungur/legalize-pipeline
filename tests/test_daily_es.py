"""Tests for Spain daily processing — affected norms resolution."""

from unittest.mock import MagicMock

import requests

from legalize.fetcher.es.daily import _parse_affected_ids, _resolve_affected_norms
from legalize.models import Disposition, Rank


# ── Real XML from BOE-A-2024-3099 (Reforma art. 49 Constitución) ──

DISPOSITION_XML_ONE_REF = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2024-3099</identificador>
    <titulo>Reforma del articulo 49 de la Constitucion</titulo>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores>
        <anterior referencia="BOE-A-1978-31229" orden="2015">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>el art. 49 de la Constitucion de 27 de diciembre de 1978</texto>
        </anterior>
      </anteriores>
      <posteriores/>
    </referencias>
  </analisis>
</documento>"""

DISPOSITION_XML_MULTI_REF = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2026-9999</identificador>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores>
        <anterior referencia="BOE-A-2015-11430" orden="1">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>el Estatuto de los Trabajadores</texto>
        </anterior>
        <anterior referencia="BOE-A-2015-11719" orden="2">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>la Ley de Empleo</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
</documento>"""

DISPOSITION_XML_NO_REFS = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2026-1000</identificador>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores/>
    </referencias>
  </analisis>
</documento>"""

DISPOSITION_XML_NO_ANALISIS = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2026-1000</identificador>
  </metadatos>
</documento>"""

DISPOSITION_XML_NON_BOE_REFS = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2026-5000</identificador>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores>
        <anterior referencia="BOE-A-2020-12345" orden="1">
          <palabra codigo="270">MODIFICA</palabra>
        </anterior>
        <anterior referencia="DOUE-L-2016-12345" orden="2">
          <palabra codigo="270">MODIFICA</palabra>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
</documento>"""


class TestParseAffectedIds:
    def test_single_reference(self):
        ids = _parse_affected_ids(DISPOSITION_XML_ONE_REF)
        assert ids == ["BOE-A-1978-31229"]

    def test_multiple_references(self):
        ids = _parse_affected_ids(DISPOSITION_XML_MULTI_REF)
        assert ids == ["BOE-A-2015-11430", "BOE-A-2015-11719"]

    def test_no_references(self):
        ids = _parse_affected_ids(DISPOSITION_XML_NO_REFS)
        assert ids == []

    def test_no_analisis_section(self):
        ids = _parse_affected_ids(DISPOSITION_XML_NO_ANALISIS)
        assert ids == []

    def test_filters_non_boe_ids(self):
        """Only BOE-A-* IDs are returned, not EU or other references."""
        ids = _parse_affected_ids(DISPOSITION_XML_NON_BOE_REFS)
        assert ids == ["BOE-A-2020-12345"]


class TestResolveAffectedNorms:
    def _make_disp(self, id_boe: str = "BOE-A-2024-3099") -> Disposition:
        return Disposition(
            id_boe=id_boe,
            title="Reform disposition",
            rank=Rank.LEY_ORGANICA,
            department="TEST",
            url_xml=f"https://www.boe.es/diario_boe/xml.php?id={id_boe}",
            affected_norms=(),
            is_new=False,
            is_correction=False,
        )

    def test_returns_ids_on_success(self):
        client = MagicMock()
        client.get_disposition_xml.return_value = DISPOSITION_XML_ONE_REF
        disp = self._make_disp()

        result = _resolve_affected_norms(client, disp)

        assert result == ["BOE-A-1978-31229"]
        client.get_disposition_xml.assert_called_once_with("BOE-A-2024-3099")

    def test_returns_empty_on_http_error(self):
        client = MagicMock()
        client.get_disposition_xml.side_effect = requests.RequestException("timeout")
        disp = self._make_disp()

        result = _resolve_affected_norms(client, disp)

        assert result == []

    def test_returns_empty_on_invalid_xml(self):
        client = MagicMock()
        client.get_disposition_xml.return_value = b"not xml at all"
        disp = self._make_disp()

        result = _resolve_affected_norms(client, disp)

        assert result == []
