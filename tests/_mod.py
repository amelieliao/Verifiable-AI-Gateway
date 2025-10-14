# FILE: tests/_mod.py
# Utility loader to import a module by file path when the project isn't installed as a package.
import importlib.util
import sys
from pathlib import Path

def load_module(path: str, name: str):
    file = Path(path).resolve()
    spec = importlib.util.spec_from_file_location(name, file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec and spec.loader, f"cannot load {file}"
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module