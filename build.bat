@RD /S /Q "build"
@RD /S /Q "dist"
python setup.py sdist bdist_wheel
twine upload --repository-url https://pypi.org/legacy/ dist/*
