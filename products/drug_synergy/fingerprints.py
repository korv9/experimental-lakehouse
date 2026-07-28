"""Morgan fingerprint computation — pure Python, no Spark.

Kept separate from the transform so the chemistry can be unit-tested and run
locally without a cluster. RDKit is imported inside the function: the module
stays importable without it, and on Spark the import happens on the executor
where it is needed.

RDKit is a *product* dependency, not a platform one. Install ``rdkit`` as a
cluster library before running the Silver pipeline.
"""
from __future__ import annotations

N_BITS = 2048
RADIUS = 2  # ECFP4


def morgan_bits(smiles: str | None, n_bits: int = N_BITS, radius: int = RADIUS) -> list[int]:
    """SMILES -> sorted indices of the set fingerprint bits.

    Returns ``[]`` for a missing or unparseable structure. That is data, not an
    error: PubChem cannot resolve every drug name, and a failed parse should
    leave the drug without a fingerprint rather than fail the pipeline.
    """
    if not smiles:
        return []
    from rdkit import Chem, RDLogger
    from rdkit.Chem import rdFingerprintGenerator

    RDLogger.DisableLog("rdApp.*")  # invalid SMILES are expected, not worth printing
    molecule = Chem.MolFromSmiles(smiles)
    if molecule is None:
        return []
    generator = rdFingerprintGenerator.GetMorganGenerator(radius=radius, fpSize=n_bits)
    return sorted(generator.GetFingerprint(molecule).GetOnBits())
