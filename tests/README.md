*NOTE: These tests are still in development, and should not be expected to properly cover all test cases yet.*
## MDF Tests
This directory contains the tests for the `mdf_forge` package.
The tests cover the `Forge` and `Query` classes.

### Running the tests
Python 3 must be installed. Go to https://www.python.org/downloads/ to download Python 3.
Pytest must also be installed. To do this, run `pip install pytest`.
After Pytest is installed, the tests can be executed by running `pytest` in this directory.

### About the tests
These tests cover the basic and advanced functionality of the `mdf_forge` package. They test each function to check that operations succeed with expected values, error with invalid values, and respect parameters appropriately.
However, the tests currently do not cover functionality in Globus Search, a service that Forge relies on. Search results are not verified. Additionally, errors from Search usually will, but are not guaranteed to, fail these tests.

