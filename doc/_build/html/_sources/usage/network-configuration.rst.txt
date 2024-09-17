Network configuration
======================

Disable SSL
-----------

A global SSL context is used when making HTTP calls using the ``copernicusmarine`` Toolbox.
For some reason, it can lead to unexpected behavior depending on your network configuration.
You can set the ``COPERNICUSMARINE_DISABLE_SSL_CONTEXT`` environment variable to any value
to globally disable the usage of SSL in the toolbox:

- on **UNIX** platforms: ``export COPERNICUSMARINE_DISABLE_SSL_CONTEXT=True``
- on **Windows** platforms: ``set COPERNICUSMARINE_DISABLE_SSL_CONTEXT=True``

Trust Env for python libraries
------------------------------

To do HTTP calls, the Copernicus Marine Toolbox uses the ``requests`` library.
By default, this library will have ``trust_env`` values set to ``True``.

If you want to deactivate this, you can set ``COPERNICUSMARINE_TRUST_ENV=False`` (default ``True``).
This can be useful, for example, if you don't want those libraries to read your ``.netrc`` file as it has been
reported that having a ``.netrc`` with a line: "default login anonymous password user@site" is incompatible
with S3 connection required by the toolbox.

Using a custom certificate path
-------------------------------

Some users reported issues with SSL certificates. You can precise a custom path to your certificate, you can set the
``COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH`` environment variable to the path of your custom certificate.

It might be useful if you want to use the global certificate instead of the one created by you conda environment for example.

Proxy
-----

To use proxies, as described in the `requests documentation <https://requests.readthedocs.io/en/latest/user/advanced/#proxies>`_,
you can use two options:

- set the ``HTTPS_PROXY`` variable. For example: ``HTTPS_PROXY="http://user:pass@some.proxy.com"``.
  It should work even with ``COPERNICUSMARINE_TRUST_ENV=False``.
- use a ``.netrc`` file but be aware that having a line: "default login anonymous password user@site" is incompatible
  with S3 connection required by the toolbox. Also note that if you have ``COPERNICUSMARINE_TRUST_ENV=True`` (the default value)
  then if ``NETRC`` environment variable is set with a specified location, the ``.netrc`` file will be read from the specified
  location there rather than from ``~/.netrc``.

Number of concurrent requests
-----------------------------

The toolbox makes many requests to STAC to be able to parse the full marine data store STAC catalog.
For that, it uses concurrent calls on one thread. It also uses this when downlaoding files for the get command.
It can be problematic to do too many requests at the same time. Or you might want to boost the download.

To limit the number of requests at the same time you can use: ``max_concurrent_requests`` argument.
See :func:`~copernicusmarine.describe` and :func:`~copernicusmarine.get`.
The default value is ``15`` and minimum value is ``1``.

.. note::
    For the ``get`` command, you can set the environment variable to ``0`` if you don't want to use the ``concurrent.futures.ThreadPoolExecutor`` at all;
    the download will be used only through ``boto3``.
