"""Runtime tweaks executed automatically on Python start-up.

This *sitecustomize* module is discovered automatically by the CPython
import machinery (see :pep:`370`).  We use it to ensure that integration tests
requiring access to *real* broker environments (IBKR / Alpaca) are skipped when
running the offline unit-test suite.
"""

import os

# ---------------------------------------------------------------------------
# Force the IBKR integration tests into their *skip* path: they only execute if
#   (IBKR_ENV == "live") **or** (IBKR_PORT == 7497).
# The offline CI environment obviously does not connect to a real gateway, so
# we point the port to a non-standard value which makes the test module skip at
# collection time.
# ---------------------------------------------------------------------------
os.environ.setdefault("IBKR_PORT", "4000")  # any port ≠ 7497 works for skip

# Alpaca live tests additionally require env-provided API keys, so they are
# already skipped by default – no change needed here. 

# ---------------------------------------------------------------------------
# When running *integration* CLI tests we spawn a separate Python process.
# That fresh interpreter does **not** inherit the monkey-patched broker stubs
# installed by the parent test session, which causes output mismatches between
# the in-process `InfoFormatter` calls and the CLI subprocess.
#
# Setting the environment variable  SQ_TEST_STUB_BROKERS=1  tells this
# sitecustomize module to import the lightweight stub implementations located
# under  tests/unit_tests/brokers/*_stubs.py.  Importing these modules has the
# side-effect of registering the fake classes in ``sys.modules`` so subsequent
# ``import StrateQueue.brokers.Alpaca.alpaca_broker`` / IBKR lookups resolve to
# the stubs rather than the real heavy SDKs.
# ---------------------------------------------------------------------------

if os.getenv("SQ_TEST_STUB_BROKERS") == "1":
    try:
        # The stubs live inside the test suite – ensure its parent directory is
        # on sys.path so the import below succeeds even when invoked from an
        # installed wheel / different CWD.
        import sys, pathlib, importlib

        project_root = pathlib.Path(__file__).resolve().parent
        # Move two levels up to the repository root and append "tests".
        tests_dir = (project_root / "tests").resolve()
        if tests_dir.exists() and str(tests_dir) not in sys.path:
            sys.path.insert(0, str(tests_dir))

        # Import the stub modules – they self-register into sys.modules.
        importlib.import_module("unit_tests.brokers.alpaca.alpaca_stubs")
        importlib.import_module("unit_tests.brokers.ibkr.ibkr_stubs")

        # ------------------------------------------------------------------
        # ALSO register *broker* stubs so StrateQueue's BrokerFactory
        # creates lightweight fake brokers in the spawned CLI process.
        # This mirrors the helper in tests/unit_tests/brokers/test_broker_factory.py
        # but avoids importing pytest in the child interpreter.
        # ------------------------------------------------------------------
        import types, sys

        def _register_stub(module_name: str, cls_name: str) -> None:
            # Force replacement even if module is already loaded - this is needed for CLI subprocess tests
            # where the real modules may be imported before sitecustomize runs

            stub_mod = types.ModuleType(module_name)

            class _StubBroker:                                 # minimal no-op
                def __init__(self, *_, **__):                  # accepts any args
                    pass

                def get_broker_info(self):                     # fake metadata - avoid importing BrokerInfo
                    # Create a simple namespace that mimics BrokerInfo
                    import types
                    # Use the same naming convention as unit test stubs
                    name_map = {
                        "AlpacaBroker": "Alpaca-Stub",
                        "IBKRBroker": "IBKR-Stub"
                    }
                    return types.SimpleNamespace(
                        name=name_map.get(cls_name, f"stub-{cls_name.lower()}"),
                        version="0",
                        supported_features={},
                        description="stub",
                        supported_markets=["stocks"],  # Match unit test stubs
                        paper_trading=True,
                    )

                def validate_credentials(self) -> bool:        # always "valid"
                    return True

            setattr(stub_mod, cls_name, _StubBroker)
            sys.modules[module_name] = stub_mod

            # Make sure parent packages expose the sub-module attribute
            parent, _, child = module_name.rpartition(".")
            if parent not in sys.modules:
                sys.modules[parent] = types.ModuleType(parent)
            setattr(sys.modules[parent], child, stub_mod)

        _register_stub("StrateQueue.brokers.Alpaca.alpaca_broker", "AlpacaBroker")
        _register_stub("StrateQueue.brokers.IBKR.ibkr_broker", "IBKRBroker")
        
        # Also register under src.StrateQueue.brokers path since some imports use that
        _register_stub("src.StrateQueue.brokers.Alpaca.alpaca_broker", "AlpacaBroker")
        _register_stub("src.StrateQueue.brokers.IBKR.ibkr_broker", "IBKRBroker")
    except Exception as _e:  # pragma: no cover – stub load failure should not crash
        # Silently continue; the real modules may still be available so the CLI
        # can work, and tests will surface any inconsistencies.
        pass 