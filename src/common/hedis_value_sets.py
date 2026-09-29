"""Simplified code lists used by the HEDIS-style measures.

Real HEDIS uses NCQA value sets (VSAC) with hundreds of codes (ICD-10, CPT, LOINC, SNOMED...).
These short lists are ONLY for learning. Do not present results as NCQA-certified.
"""

DIABETES_DX_PREFIX = "E11"          # Type 2 diabetes mellitus
HYPERTENSION_DX = ("I10",)          # Essential hypertension
A1C_COMPONENT = "HEMOGLOBIN A1C"    # result component name in ORDER_RESULTS
A1C_CPT = "83036"
COLONOSCOPY_CPT = ("45378",)
FIT_CPT = ("82274",)


def sql_in(values) -> str:
    return ", ".join(f"'{v}'" for v in values)
