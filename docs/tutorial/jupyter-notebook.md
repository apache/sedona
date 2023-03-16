# Python Jupyter Notebook Examples

Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=binder) and play the interactive Sedona Python Jupyter Notebook immediately!

Sedona Python provides a number of [Jupyter Notebook examples](https://github.com/apache/sedona/blob/master/binder/).


Please use the following steps to run Jupyter notebook with Pipenv on your machine

1. Clone Sedona GitHub repo or download the source code
2. Install Sedona Python from PyPI or GitHub source: Read [Install Sedona Python](../../setup/install-python/#install-sedona) to learn.
3. Prepare spark-shaded jar: Read [Install Sedona Python](../../setup/install-python/#prepare-spark-shaded-jar) to learn.
4. Setup pipenv python version. Please use your desired Python version.
```bash
cd binder
pipenv --python 3.8
```
5. Install dependencies
```bash
cd binder
pipenv install
```
6. Install jupyter notebook kernel for pipenv
```bash
pipenv install ipykernel
pipenv shell
```
7. In the pipenv shell, do
```bash
python -m ipykernel install --user --name=apache-sedona
```
8. Setup environment variables `SPARK_HOME` and `PYTHONPATH` if you didn't do it before. Read [Install Sedona Python](../../setup/install-python/#setup-environment-variables) to learn.
9. Launch jupyter notebook: `jupyter notebook`
10. Select Sedona notebook. In your notebook, Kernel -> Change Kernel. Your kernel should now be an option.