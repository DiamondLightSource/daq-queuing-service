import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_URL = "https://github.com/DiamondLightSource/ulims-json-schemas.git"
BUILD_DIR = Path("build")  # relative to the repo root
OUTPUT_ROOT = Path("src/daq_queuing_service/plugins")


def clone_repo(dest: Path, ref: str) -> None:
    """Clone the repo and check out ref (branch, tag, or commit hash)."""
    subprocess.run(["git", "clone", REPO_URL, str(dest)], check=True)
    subprocess.run(["git", "-C", str(dest), "checkout", ref], check=True)


def relative_to_instrument(
    schema: Path, schemas_root: Path, instrument: str
) -> Path | None:
    """Return the schema's path with the instrument segment removed.

    schemas/experiment_definition/i15-1/cobra/2.0.0.json
    -> experiment_definition/cobra/2.0.0.json
    """
    parts = schema.relative_to(schemas_root).parts
    if instrument not in parts:
        return None
    idx = parts.index(instrument)
    return Path(*parts[:idx], *parts[idx + 1 :])


def python_path(rel: Path) -> Path:
    """Map cobra/2.0.0.json to cobra/2_0_0.py so the package is importable."""
    *dirs, name = rel.parts
    stem = name.removesuffix(".json").replace("-", "_").replace(".", "_")
    return Path(*(d.replace("-", "_") for d in dirs), f"{stem}.py")


def generate_model(schema: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "datamodel-codegen",
            "--input",
            str(schema),
            "--input-file-type",
            "jsonschema",
            "--output",
            str(output),
            "--output-model-type",
            "pydantic_v2.BaseModel",
            "--formatters",
            "black",
            "isort",
        ],
        check=True,
    )


def main(instrument: str = "i15-1", ref: str = "main") -> None:
    BUILD_DIR.mkdir(exist_ok=True)
    package = instrument.replace("-", "_")
    output_base = OUTPUT_ROOT / package / "generated"
    failures: list[Path] = []

    # The clone is removed when the block exits, even on errors.
    with tempfile.TemporaryDirectory(dir=BUILD_DIR, prefix="ulims-") as tmp:
        repo = Path(tmp) / "ulims-json-schemas"
        clone_repo(repo, ref)

        schemas_root = repo / "schemas"
        for schema in sorted(schemas_root.rglob("*.json")):
            rel = relative_to_instrument(schema, schemas_root, instrument)
            if rel is None:
                continue

            output = output_base / python_path(rel)
            try:
                generate_model(schema, output)
                print(f"generated {output}")
            except subprocess.CalledProcessError:
                failures.append(schema)
                print(f"failed: {schema}", file=sys.stderr)

    if failures:
        print(
            f"{len(failures)} schema(s) failed: "
            + str([str(failure) for failure in failures]),
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate Pydantic models from ulims JSON schemas."
    )
    parser.add_argument(
        "--instrument",
        default="i15-1",
        help="Instrument directory name (default: i15-1)",
    )
    parser.add_argument(
        "--ref",
        default="main",
        help="Branch, tag, or commit hash to check out (default: main)",
    )
    args = parser.parse_args()
    main(instrument=args.instrument, ref=args.ref)
