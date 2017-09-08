# Forge Installation
## Via PyPI
You will need to have Python 3 and pip installed for this method.

Install Forge with `pip`:
```bash
pip3 install -u mdf_forge
```

## Via GitHub
This method is only recommended if you want to have the docs and examples locally.

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
pip3 install -e .
```


# Refinery Installation
Please note that Refinery is designed for ingesting data into MDF. Refinery is *not* required to access or search data in MDF.
## Via GitHub
You will need to have git, Python 3, and pip to install Refinery via GitHub.
Start by following the instructions above to install Forge via GitHub.
```bash
git clone https://github.com/materials-data-facility/forge.git
cd forge
pip3 install -e .
```
Then, enter the `refinery` directory:
```bash
cd ..
cd refinery
```
Finally, use `pip` to install Refinery:
```bash
pip3 install -e .
```
