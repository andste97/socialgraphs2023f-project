# socialgraphs2023f-project
Repository for assignments and group project for course Social graphs and interactions in autumn semester 2023


## Installing Wikichatter

If you use a venv you can follow this guide:

1. clone the dependency into a new repo:  ```git clone https://github.com/mediawiki-utilities/python-mwchatter.git```
2. Best to create & activate a new venv in the folder
3. install dependencies: `pip install -r requirements.txt`
4. run `python setup.py build`. This will build the package and put it in the folder build.
5. Copy the folder `build/lib/wikichatter` into the folder `<venv-root-folder>/lib/<python-dist-folder>/site-packages`

Installing using conda may differ.