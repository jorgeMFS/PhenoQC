def main():
    # Lazy import so test modules that import phenoqc.gui.views
    # do not pull heavy GUI dependencies at import time.
    from .gui import main as _main
    return _main()

__all__ = ["main"]
