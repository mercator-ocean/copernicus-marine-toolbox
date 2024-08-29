from sphinx.builders.html import StandaloneHTMLBuilder

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Copernicus Marine Toolbox"
copyright = "2024, Mercator Ocean International"
author = "Mercator Ocean International"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx_click",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

pygments_style = "sphinx"
pygments_dark_style = "monokai"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = ["_static"]

# -- Options for the furo theme -----------------------------------------------
# https://pradyunsg.me/furo/settings/

# html_theme_options = {
#     "light_css_variables": {
#         "color-sidebar-background": "#5c7bd1",
#         "color-sidebar-background-hover": "#4a6aa8",
#         "link-color-sidebar-text": "#ffffff",
#     },
# }

# -- Options for different image types -------------------------------------------
# https://stackoverflow.com/questions/45969711/sphinx-doc-how-do-i-render-an-animated-gif-when-building-for-html-but-a-png-wh

StandaloneHTMLBuilder.supported_image_types = [
    "image/svg+xml",
    "image/gif",
    "image/png",
    "image/jpeg",
]
