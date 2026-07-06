"""Hadith authenticity-grade normalization.

Source corpora record a hadith's grade as free text in whatever script the
upstream used: Arabic (``صحيح``), transliterated English (``Sahih``,
``Da'if``), or descriptive English (``authentic``, ``weak``). The graph stores
that raw string verbatim so the original is never lost, but a raw Arabic /
mixed-script value is not a stable key to filter, facet, or display on. This
module maps any of those raw forms onto the canonical :class:`HadithGrade`
vocabulary so a normalized *display* field can sit alongside the raw one
(da#148 integrity sweep — "grade stored as raw Arabic, needs normalized
display").

The function is deliberately conservative: anything it does not recognise maps
to :data:`HadithGrade.UNKNOWN` rather than guessing, so a normalized value is
always a real enum member.

Batch ⇄ streaming parity (ip#119)
---------------------------------
This is a **verbatim mirror** of the batch load path's canonical
``normalize_grade()`` in ``noorinalabs-data-acquisition/src/utils/grade.py``
(added by da#150). The streaming normalize worker
(``workers/normalize/processor.py``) calls this so a hadith's
``grade_normalized`` value is identical no matter which path — batch load or
Kafka stream — produced the node (da#153 ADR-004 item #4, meta main#723). Both
repos share the same :class:`HadithGrade` vocabulary and an equivalent
:func:`normalize_arabic` pipeline, so the mapping is value-stable across the
two. Keep the two copies in lockstep.
"""

from __future__ import annotations

from src.models.enums import HadithGrade
from src.utils.arabic import normalize_arabic

__all__ = ["normalize_grade"]

# Match order matters: the ``li-ghayrihi`` ("…by virtue of corroboration")
# qualifiers are checked before the bare grade so ``صحيح لغيره`` does not match
# the plain ``sahih`` rule first. Each entry is ``(needles, grade)`` — the raw
# value matches if ANY needle is a substring of the normalized text. Arabic
# needles are pre-normalized with :func:`normalize_arabic` (the same pipeline
# applied to the input) so diacritics/alif/taa-marbuta variants all collapse.
_GRADE_RULES: list[tuple[tuple[str, ...], HadithGrade]] = [
    (
        (normalize_arabic("صحيح لغيره"), "sahih li ghayrihi", "sahih li-ghayrihi"),
        HadithGrade.SAHIH_LI_GHAYRIHI,
    ),
    (
        (normalize_arabic("حسن لغيره"), "hasan li ghayrihi", "hasan li-ghayrihi"),
        HadithGrade.HASAN_LI_GHAYRIHI,
    ),
    ((normalize_arabic("موضوع"), "mawdu", "fabricat"), HadithGrade.MAWDU),
    ((normalize_arabic("ضعيف"), "daif", "da'if", "weak"), HadithGrade.DAIF),
    ((normalize_arabic("صحيح"), "sahih", "authentic"), HadithGrade.SAHIH),
    ((normalize_arabic("حسن"), "hasan", "good"), HadithGrade.HASAN),
]


def normalize_grade(raw: str | None) -> str:
    """Map a raw grade string (Arabic or English, any corpus) to a HadithGrade.

    Returns the :class:`HadithGrade` *value* (a ``str``) so it can be written
    straight onto a node property. An empty / non-string / unrecognised value
    yields :data:`HadithGrade.UNKNOWN` — the raw field still carries the
    original, so nothing is lost.
    """
    if not raw or not isinstance(raw, str):
        return HadithGrade.UNKNOWN.value
    # Normalize the Arabic side and lowercase the Latin side so one pass of
    # substring checks covers both scripts.
    text = normalize_arabic(raw).lower()
    for needles, grade in _GRADE_RULES:
        if any(needle and needle in text for needle in needles):
            return grade.value
    return HadithGrade.UNKNOWN.value
