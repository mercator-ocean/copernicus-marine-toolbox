import toml
from sphinx.builders.html import StandaloneHTMLBuilder

data = toml.load("../pyproject.toml")

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Copernicus Marine Toolbox"
copyright = "2024, Mercator Ocean International"
author = "Mercator Ocean International"
version = data["project"]["version"]

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx_click",
    "numpydoc",
    "sphinx_copybutton",
    "myst_nb",
]
numpydoc_show_class_members = False

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "__pycache__"]

pygments_style = "sphinx"
pygments_dark_style = "monokai"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = ["_static"]
html_favicon = "_static/favicon_cmems.ico"
html_css_files = ["css/custom.css"]

# -- Options for the furo theme -----------------------------------------------
# https://pradyunsg.me/furo/customisation/

html_logo = "_static/favicon_cmems.ico"

html_theme_options = {
    "light_css_variables": {"color-brand-primary": "#607fad"},
}

html_sidebars = {
    "**": [
        "sidebar/brand.html",
        "sidebar/search.html",
        "sidebar/scroll-start.html",
        "sidebar/navigation.html",
        "sidebar/ethical-ads.html",
        "sidebar/scroll-end.html",
        "sidebar/github.html",
    ]
}

# -- Options for different image types -------------------------------------------
# https://stackoverflow.com/questions/45969711/sphinx-doc-how-do-i-render-an-animated-gif-when-building-for-html-but-a-png-wh

StandaloneHTMLBuilder.supported_image_types = [
    "image/svg+xml",
    "image/gif",
    "image/png",
    "image/jpeg",
]

# -- Options for myst_nb --------------------------------------------------
# https://myst-nb.readthedocs.io/en/latest/configuration.html#config-intro

nb_execution_mode = "off"

# -- Custom RST epilog --------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-rst_epilog
# Allows us to have the version dynamically updated in the documentation

rst_epilog = """
.. |download_macos_arm64| replace:: `copernicusmarine_macos-arm64 <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases/download/v{0}/copernicusmarine_macos-arm64.cli>`__
.. |download_macos_x86| replace:: `copernicusmarine_macos-x86_64 <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases/download/v{0}/copernicusmarine_macos-x86_64.cli>`__
.. |download_linux_235| replace:: `copernicusmarine_linux <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases/download/v{0}/copernicusmarine_linux-glibc-2.35.cli>`__
.. |download_linux_239| replace:: `copernicusmarine_linux_2.39 <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases/download/v{0}/copernicusmarine_linux-glibc-2.39.cli>`__
.. |download_windows| replace:: `copernicusmarine <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases/download/v{0}/copernicusmarine.exe>`__
""".format(
    version
)
