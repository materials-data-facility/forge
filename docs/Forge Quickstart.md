# Forge Quickstart Guide
## Install Forge
The first step is to install Forge. Detailed instructions are available in the [Installation Guide](Installation Guide.md). You only need to install Forge.

## Import Forge
Once Forge is installed, it is available for your Python scripts or Jupyter notebooks.

Import and start Forge like this:
```python 3
from mdf_forge import forge
# You don't have to use the name "mdf" but we do for consistency.
mdf = forge.Forge()
```

## Log in
The first time you use Forge, you will have to log in through Globus Auth.
```
It looks like this is the first time you're accessing this client.
Please log in to Globus at this link:
  https://auth.globus.org/v2/oauth2/.../

Copy and paste the authorization code here: ____________________
Thanks!
```
Forge caches your token, so after the first time you log in, you will only have to redo the process when the token expires or you delete it.

## Search
Now you can access data in MDF. The simplest way to fetch data is to use `search()`.
```python 3
mdf.search("DFT")
```
