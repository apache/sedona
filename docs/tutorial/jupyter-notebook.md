# Python Jupyter Notebook Examples

Sedona Python provides two Jupyter Notebook examples: [Sedona core](https://github.com/apache/incubator-sedona/blob/master/python/ApacheSedonaCore.ipynb) and [Sedona SQL](https://github.com/apache/incubator-sedona/blob/master/python/ApacheSedonaSQL.ipynb)


Please use the following steps to run Jupyter notebook with Pipenv

1. Clone Sedona GitHub repo or download the source code
2. Install Sedona Python from PyPi or GitHub source: Read [Install Sedona Python](/download/overview/#install-sedona) to learn.
3. Prepare python-adapter jar: Read [Install Sedona Python](/download/overview/#prepare-python-adapter-jar) to learn.
4. Setup pipenv python version. For Spark 3.0, Sedona supports 3.7 - 3.9
```bash
cd python
pipenv --python 3.8
```
5. Install dependencies
```bash
cd python
pipenv install
```
6. Install jupyter notebook kernel for pipenv
```bash
pipenv install ipykernel
pipenv shell
```
7. In the pipenv shell, do
```bash
python -m ipykernel install --user --name=my-virtualenv-name
```
8. Setup environment variables `SPARK_HOME` and `PYTHONPATH` if you didn't do it before. Read [Install Sedona Python](/download/overview/#setup-environment-variables) to learn.
9. Launch jupyter notebook: `jupyter notebook`
10. Select Sedona notebook. In your notebook, Kernel -> Change Kernel. Your kernel should now be an option.