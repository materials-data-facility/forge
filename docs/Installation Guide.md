# Forge Installation
## Via GitHub
You will need to have git, Python 3, and pip to install Forge via GitHub.

Start by cloning the GitHub repository:
```bash
git clone https://github.com/materials-data-facility/forge.git
```
Then, enter the `forge` directory:
```bash
cd forge
```
Finally, use `pip` to install Forge:
```bash
pip install -e .
```

## Via PyPI
**Coming soon, but *currently unsupported!***

You will need to have Python 3 and pip installed for this method.

Install Forge with `pip`:
```bash
pip install -u mdf_forge
```


# Refinery Installation
Please note that Refinery is designed for ingesting data into MDF. Refinery is *not* required to access or search data in MDF.
## Via GitHub
You will need to have git, Python 3, and pip to install Refinery via GitHub.
Start by following the instructions above to install Forge via GitHub.
```bash
git clone https://github.com/materials-data-facility/forge.git
cd forge
pip install -e .
```
Then, enter the `refinery` directory:
```bash
cd ..
cd refinery
```
Finally, use `pip` to install Refinery:
```bash
pip install -e .
```
