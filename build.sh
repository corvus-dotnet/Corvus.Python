#!/bin/bash
set -e

poetry run flake8
poetry run pytest --cov-report=term-missing:skip-covered --junitxml=./pytest.xml --cov=src/corvus_python tests/ | tee ./pytest-coverage.txt

echo "Injecting the version number ${PACKAGE_VERSION_NUMBER} into pyproject.toml file before building the package..."
sed -i "s/version = \"0\.1\.0\"/version = \"${PACKAGE_VERSION_NUMBER}\"/" ./pyproject.toml
echo "Modified line in file:"
cat pyproject.toml | grep version
echo "Running build to generate wheel file..."
poetry build
echo "Contents of output directory:"
ls -l ./dist