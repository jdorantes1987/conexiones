import importlib
import sys

TEST_MODULES = [
    "tests.test_cursor_wrapper",
    "tests.test_database_connector",
]

if __name__ == "__main__":
    failures = 0
    for mod in TEST_MODULES:
        try:
            m = importlib.import_module(mod)
            for name in dir(m):
                if name.startswith("test_") and callable(getattr(m, name)):
                    try:
                        print(f"Running {mod}.{name}()...", end=" ")
                        getattr(m, name)()
                        print("OK")
                    except AssertionError as e:
                        failures += 1
                        print("FAIL", e)
                    except Exception as e:
                        failures += 1
                        print("ERROR", e)
        except Exception as e:
            print(f"Could not import {mod}: {e}")
            failures += 1

    if failures:
        print(f"\n{failures} test(s) failed")
        sys.exit(1)
    else:
        print("\nAll tests passed")
        sys.exit(0)
