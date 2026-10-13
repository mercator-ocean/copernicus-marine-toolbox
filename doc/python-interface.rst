=================
Python interface
=================

.. automodule:: copernicusmarine
   :members: subset, get, describe, open_dataset, read_dataframe, login, subset_split_on

Polygon selection
-----------------

Like :func:`~copernicusmarine.subset`, :func:`~copernicusmarine.open_dataset`
and :func:`~copernicusmarine.read_dataframe` accept ``polygons_file`` as a
string or ``pathlib.Path``. Supported formats are GeoJSON, Shapefile,
GeoPackage, and KML. Install the optional dependencies with
``pip install copernicusmarine[extra]``.

.. code-block:: python

   import copernicusmarine

   dataset = copernicusmarine.open_dataset(
       dataset_id="cmems_mod_glo_phy_anfc_0.083deg_P1M-m",
       polygons_file="area.geojson",
   )
   dataframe = copernicusmarine.read_dataframe(
       dataset_id="cmems_mod_glo_phy_anfc_0.083deg_P1M-m",
       polygons_file="area.geojson",
   )

The polygon bounding box is used to retrieve data, then the polygons are
applied to the result. Polygon selection replaces other geographical bounds;
time and depth selections still apply. Gridded data are clipped before
conversion to a DataFrame, preserving its coordinate index and masked cells
outside the polygons. Sparse DataFrames contain only observations within
the polygons. Polygon selection is not supported for ``originalGrid`` datasets.

See :ref:`subset-polygons` for more information.
