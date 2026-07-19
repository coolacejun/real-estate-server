"""Build compatible PNU candidates across legal-dong code changes.

The mappings below are based on the MOIS legal-dong code changes effective
2026-07-01. Keep the original PNU first so current data always wins, then try
the corresponding current or legacy code when a data source is out of sync.

Source: https://www.mois.go.kr/frt/bbs/type001/commonSelectBoardArticle.do?bbsId=BBSMSTR_000000000052&nttId=127039
"""

from __future__ import annotations


# Gwangju and Jeonnam were reorganized as Jeonnam-Gwangju Integrated Special
# Metropolitan City. Every affected legal-dong code can be translated by its
# first five digits.
_JEONNAM_GWANGJU_PREFIX_PAIRS: tuple[tuple[str, str], ...] = (
    ("12110", "46110"),
    ("12130", "46130"),
    ("12150", "46150"),
    ("12170", "46170"),
    ("12190", "46230"),
    ("12210", "29110"),
    ("12240", "29140"),
    ("12270", "29155"),
    ("12300", "29170"),
    ("12330", "29200"),
    ("12710", "46710"),
    ("12720", "46720"),
    ("12730", "46730"),
    ("12740", "46770"),
    ("12750", "46780"),
    ("12760", "46790"),
    ("12770", "46800"),
    ("12780", "46810"),
    ("12790", "46820"),
    ("12800", "46830"),
    ("12810", "46840"),
    ("12820", "46860"),
    ("12830", "46870"),
    ("12840", "46880"),
    ("12850", "46890"),
    ("12860", "46900"),
    ("12870", "46910"),
)


# Incheon's 2026 reorganization moved legal dongs among Jemulpo, Yeongjong,
# Seohae, and Geomdan districts, so exact ten-digit mappings are required.
_INCHEON_LEGAL_CODE_PAIRS: tuple[tuple[str, str], ...] = (
    ("2812510100", "2814010100"),
    ("2812510200", "2814010200"),
    ("2812510300", "2814010300"),
    ("2812510400", "2814010400"),
    ("2812510500", "2814010500"),
    ("2812510600", "2814010600"),
    ("2812510700", "2814010700"),
    ("2812510800", "2811010100"),
    ("2812510900", "2811010200"),
    ("2812511000", "2811010300"),
    ("2812511100", "2811010400"),
    ("2812511200", "2811010500"),
    ("2812511300", "2811010600"),
    ("2812511400", "2811010700"),
    ("2812511500", "2811010800"),
    ("2812511600", "2811010900"),
    ("2812511700", "2811011000"),
    ("2812511800", "2811011100"),
    ("2812511900", "2811011200"),
    ("2812512000", "2811011300"),
    ("2812512100", "2811011400"),
    ("2812512200", "2811011500"),
    ("2812512300", "2811011600"),
    ("2812512400", "2811011700"),
    ("2812512500", "2811011800"),
    ("2812512600", "2811011900"),
    ("2812512700", "2811012000"),
    ("2812512800", "2811012100"),
    ("2812512900", "2811012200"),
    ("2812513000", "2811012300"),
    ("2812513100", "2811012400"),
    ("2812513200", "2811012500"),
    ("2812513300", "2811012600"),
    ("2812513400", "2811012700"),
    ("2812513500", "2811012800"),
    ("2812513600", "2811012900"),
    ("2812513700", "2811013000"),
    ("2812513800", "2811013100"),
    ("2812513900", "2811013200"),
    ("2812514000", "2811013300"),
    ("2812514100", "2811013400"),
    ("2812514200", "2811013500"),
    ("2812514300", "2811013600"),
    ("2812514400", "2811013700"),
    ("2812514500", "2811013800"),
    ("2812514600", "2811013900"),
    ("2812514700", "2811014000"),
    ("2812514800", "2811014100"),
    ("2812514900", "2811014200"),
    ("2812515000", "2811014300"),
    ("2812515100", "2811014400"),
    ("2815510100", "2811014500"),
    ("2815510200", "2811014600"),
    ("2815510300", "2811014700"),
    ("2815510400", "2811014800"),
    ("2815510500", "2811014900"),
    ("2815510600", "2811015000"),
    ("2815510700", "2811015100"),
    ("2815510800", "2811015200"),
    ("2827510100", "2826010300"),
    ("2827510200", "2826010400"),
    ("2827510300", "2826010500"),
    ("2827510400", "2826010600"),
    ("2827510500", "2826010700"),
    ("2827510600", "2826010800"),
    ("2827510700", "2826010900"),
    ("2827510800", "2826011000"),
    ("2827510900", "2826011100"),
    ("2827511000", "2826011200"),
    ("2827511100", "2826012200"),
    ("2829010100", "2826010100"),
    ("2829010200", "2826010200"),
    ("2829010300", "2826011300"),
    ("2829010400", "2826011400"),
    ("2829010500", "2826011500"),
    ("2829010600", "2826011700"),
    ("2829010700", "2826011800"),
    ("2829010800", "2826011900"),
    ("2829010900", "2826012000"),
    ("2829011000", "2826012100"),
)


def _build_bidirectional_aliases(
    pairs: tuple[tuple[str, str], ...],
) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for current, legacy in pairs:
        if current in aliases or legacy in aliases:
            raise ValueError("PNU alias mapping contains a duplicate code")
        aliases[current] = legacy
        aliases[legacy] = current
    return aliases


_REGION_PREFIX_ALIASES = _build_bidirectional_aliases(
    _JEONNAM_GWANGJU_PREFIX_PAIRS
)
_LEGAL_CODE_ALIASES = _build_bidirectional_aliases(_INCHEON_LEGAL_CODE_PAIRS)


def _legal_code_candidates(legal_code: str) -> list[str]:
    candidates = [legal_code]

    exact_alias = _LEGAL_CODE_ALIASES.get(legal_code)
    if exact_alias:
        candidates.append(exact_alias)

    prefix_alias = _REGION_PREFIX_ALIASES.get(legal_code[:5])
    if prefix_alias:
        alias = f"{prefix_alias}{legal_code[5:]}"
        if alias not in candidates:
            candidates.append(alias)

    return candidates


def _land_flag_candidates(flag: str) -> tuple[str, ...]:
    if flag == "0":
        return ("0", "1")
    if flag == "1":
        return ("1", "0")
    if flag == "2":
        return ("2", "1")
    return (flag,)


def pnu_query_candidates(pnu: str) -> list[str]:
    """Return normalized PNU candidates, preserving the input as first choice."""

    raw = str(pnu or "").strip()
    if not raw:
        return []

    digits = "".join(character for character in raw if character.isdigit())
    if len(digits) < 19:
        return [raw]

    normalized = digits[-19:]
    candidates: list[str] = []
    for legal_code in _legal_code_candidates(normalized[:10]):
        for land_flag in _land_flag_candidates(normalized[10]):
            candidate = f"{legal_code}{land_flag}{normalized[11:]}"
            if candidate not in candidates:
                candidates.append(candidate)

    return candidates
