# Estonia (Riigi Teataja) — PDF annex investigation

Goal: figure out whether the PDF blobs that ride along inside Estonian law XML
(`<lisaViide><fail failVorming="pdf">…base64…</fail></lisaViide>`) are mostly
disposable graphics/forms or actual legal data we should preserve in the repo.

Method: resolve well-known law abbreviations via
`https://www.riigiteataja.ee/akt/{ABBR}` to get the `globaalID`, fetch the
matching `.xml`, count `<lisaViide>` and PDF `<fail>` elements, then for the
laws that actually have PDFs, decode the first 1–2 base64 blobs and run
`pdftotext -layout` on them. All raw artifacts are kept under
`tests/fixtures/ee/_investigation/` for reproducibility.

## 1. Coverage

25 abbreviations were tried from the original list (tax, traffic, health,
environment, social, education, construction, food/agriculture domains).

| Result | Count | Notes |
|---|---|---|
| Resolved to a `globaalID` and XML inspected | **22** | the 22 abbreviations that the RT short-link service knew |
| Did not resolve (404 / no `<id>.xml` link in HTML) | 3 | `JaatS`, `LoomKS`, `VetK` — names have changed or use a different short-link |
| Of the 22 resolved, with **PDF annexes** | **3** | `MS`, `KutS`, `EhS` |
| Of the 22 resolved, with `<lisaViide>` but **no** PDF (xml-only annex) | 1 | `LKindlS` — annex is structured XML, not a base64 blob |

A second pass with 22 alternative abbreviations (`LKS`, `RPS`, `MKS`, `RHS`,
`PankrS`, `TMS`, …) found **2 more laws with PDFs**: `LKS` (Looduskaitseseadus,
1 PDF) and `RPS` (Raamatupidamise seadus, 2 PDFs).

The pre-existing fixture `sample_large_110102012005.xml` (a Finance-Ministry
regulation on alcohol/tobacco tax stamps) contributes 3 more PDFs that the user
already extracted.

**Aggregate: 6 laws with PDFs, 12 PDFs classified.**

Note: two of the abbreviations resolved to *different* laws than expected
(estonian abbreviations clash). `MS` resolved to **Metsaseadus** (Forest Act),
not Meresõiduohutuse seadus. `LKS` resolved to **Looduskaitseseadus** (Nature
Protection Act), not Loomakaitseseadus. The findings still hold — we're
sampling annex *content*, not specific laws.

## 2. Per-PDF classification

Categories: **A** = pure graphic design, **B** = blank form template,
**C** = table/structured data with legal meaning, **D** = other.

| # | Law | Annex file | Cat | What it is |
|---|---|---|---|---|
| 1 | sample_large `110102012005` (Min. Finance, alcohol/tobacco stamps) | `01_…Lisa1.pdf` | **A** | Visual design of an alcohol excise stamp (image only) |
| 2 | same | `02_Lisa.pdf` | **A** | Visual design of a tobacco excise stamp (image only) |
| 3 | same (regulation `102122010007`) | `03_…lisa_3.pdf` | **B** | Blank "Alkoholi maksumärkide saateleht" delivery slip — empty boxes |
| 4 | **Metsaseadus** `119122024006`, lisa 2 | `MS_1_lisa2.pdf` | **C** | Damage-calculation table: euro fines per illegally cut tree by stump diameter and species (4 columns × 16 rows) |
| 5 | **Metsaseadus** lisa 3 | `MS_2_Lisa3.pdf` | **C** | Damage table for reducing forest basal area below the legal minimum, by species and stand age |
| 6 | **Kutseseadus** `113032019010`, lisa 1 | `KutS_1_Lisa_1.pdf` | **C** | The official 8-level Estonian Qualifications Framework (knowledge / skills / responsibility per level) |
| 7 | **Kutseseadus** lisa 2 | `KutS_2_Lisa_2.pdf` | **C** | The 5 vocational qualification levels under the 2001 Kutseseadus |
| 8 | **Ehitusseadustik** `118032026007`, lisa 1 | `EhS_1_Lisa1_28032026.pdf` | **C** | Master table for *when a building permit / building notice / project documentation is required*, by building type, footprint and works category |
| 9 | **Ehitusseadustik** lisa 2 | `EhS_2_Lisa2_28032026.pdf` | **C** | Same matrix for *use permits / use notices* (kasutusluba / kasutusteatis) |
| 10 | **Looduskaitseseadus** `128012026005`, lisa | `LKS_1_…_lisa.pdf` | **C** | Selection-cutting limits in protected areas: minimum basal area (m²/ha) per stand and quality class |
| 11 | **Raamatupidamise seadus** `110072025003`, lisa 1 | `RPS_1_…_lisa1.pdf` | **C** | Mandatory Estonian balance-sheet schema (line items in legal order) |
| 12 | **Raamatupidamise seadus** lisa 2 | `RPS_2_…_Lisa_2.pdf` | **C** | Mandatory profit-and-loss statement schemas (Skeem 1 + Skeem 2) |

**Totals:** A=2, B=1, **C=9**, D=0 (out of 12 PDFs).

## 3. Category C examples — actual legal content (quotes)

> **Metsaseadus, lisa 2 — environmental damage scale per illegally cut tree**
> ```
> Stump diameter (cm)   Pine/spruce/birch   Oak/ash/elm   Aspen/lime   Grey alder/willow
> 14.1–18                  5,56               6            4,15           2,81
> 38.1–42                  34                67,45        25,50          17
> 58.1–62                  92,35             212          69,25          46,20
> ```

> **Ehitusseadustik, lisa 1 — when do you need a building permit**
> ```
> Footprint 0–60 m² and over 5 m high  →  Ehitusluba (permit required)
> Footprint over 60 m²                  →  Ehitusluba
> Footprint 20–60 m² up to 5 m high     →  Ehitusteatis + project documentation
> ```

> **Kutseseadus, lisa 1 — Estonian Qualifications Framework, level 7**
> > "very specialised knowledge, partly at the forefront of the field, on which
> > original thinking is based; specialised problem-solving skills required for
> > research and/or innovation activity to develop new knowledge and procedures…"

> **Raamatupidamise seadus, lisa 1 — mandatory balance-sheet line items**
> ```
> *VARAD → *Käibevarad → Nõuded ja ettemaksed
>   1. Nõuded ostjate vastu
>   2. Nõuded seotud osapoolte vastu
>   3. Maksude ettemaksed ja tagasinõuded
>   …
> ```

> **Looduskaitseseadus, lisa — minimum basal area after selection cutting**
> ```
> Stand                          1A   1   2   3   4   5   5A
> Conifers / hardwoods           26  24  22  20  18  16  14
> Softwoods                      22  20  18  16  14  12  10
> ```

## 4. Recommendation

**Do NOT skip PDFs blindly, and do NOT just link them as opaque blobs.**

Out of 12 PDFs sampled, **9/12 (75 %) carry primary, table-shaped legal content**
(fee schedules, the Estonian Qualifications Framework, the building-permit
matrix, the statutory balance-sheet template, environmental damage scales).
Dropping them would mean shipping the rest of the law in Markdown but losing
the bit that judges and accountants actually look up. The graphic-only PDFs
(2 stamp designs + 1 blank delivery slip) live exclusively in regulations of
the *Rahandusministri määrus* type, not in the major statutes.

Concretely, my recommendation is **option (c) with a fallback to (b)**:

1. **For every PDF annex, also save the raw file** under `ee/lisa/{lawId}_lisaN.pdf`
   in the country repo. Cheap, deterministic, future-proof.
2. **Run `pdftotext -layout` and embed the result** as a Markdown table inside
   the parent law file (or as a sibling `…_lisaN.md`). The Estonian PDFs are
   genuinely table-shaped, with very few merged cells and no scanned images
   in the C-category set, so `pdftotext -layout` already gives an aligned text
   grid that converts cleanly to a pipe table with a small post-processor.
3. **Heuristic gate** (so we don't ship junk for the A/B cases): only attempt
   the Markdown conversion if the extracted text contains at least one line
   with three or more whitespace-separated numeric tokens, OR more than ~30
   non-empty lines of running text. The 3 stamp/form PDFs from
   `sample_large_110102012005.xml` fall below both thresholds in our sample
   (≤11 lines, no numeric data), so they'd correctly fall back to "raw PDF
   linked from the law file" and not pollute the Markdown.
4. **Mark each rendered annex** in the Markdown with a `<!-- source: lisa{N}.pdf
   (sha256:…) -->` comment so reprocessing is reproducible and reformatting
   bugs can be flagged in the integrity check.

Either way, the existing fetcher code path that already detects
`<fail failVorming="pdf">` should be wired into the transformer/committer
pipeline; treating PDFs as a separate file type rather than dropping them is
clearly the right call for Estonia.

---

**Reproduction artifacts** (all under `tests/fixtures/ee/_investigation/`):

- `resolve.py`, `resolve_extra.py` — abbreviation → globaalID resolver (curl-based)
- `extract_pdfs.py` — XML download + base64 PDF extraction + pdftotext driver
- `dump_existing_samples.py` — pdftotext over the 3 pre-existing fixture PDFs
- `catalog.json`, `catalog_extra.json` — per-abbreviation lisaViide/PDF counts
- `pdf_index.json` — structured per-PDF findings
- `xmls/` — 5 raw XML downloads (MS, KutS, EhS, LKS, RPS)
- `pdfs/` — 9 freshly extracted PDFs
- `texts/` — 12 `pdftotext -layout` outputs (50 lines each, 9 new + 3 SAMPLE)
