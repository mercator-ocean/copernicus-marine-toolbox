Environment variables
=======================

This page list all the environment variables read by the toolbox
with a short description.

We also provide an example on how to set the environment variables,
but it can be done in many ways.

Some of those variables are more extensively described
in the :ref:`network configuration page <network-configuration>`.

``COPERNICUSMARINE_SERVICE_USERNAME``
---------------------------------------

This will be read as the username to authenticate the Copernicus Marine Services.
It has precedence over configuration files. See :ref:`login usage page <login-page>`.

It can be set this way:

- on **UNIX** platforms: ``export COPERNICUSMARINE_SERVICE_USERNAME=<my username>``
- on **Windows** platforms: ``set COPERNICUSMARINE_SERVICE_USERNAME=<my username>``

``COPERNICUSMARINE_SERVICE_PASSWORD``
--------------------------------------

This will be read as the password to authenticate the Copernicus Marine Services.
It has precedence over configuration files. See :ref:`login usage page <login-page>`.

It can be set this way:

- on **UNIX** platforms: ``export COPERNICUSMARINE_SERVICE_PASSWORD=<my password>``
- on **Windows** platforms: ``set COPERNICUSMARINE_SERVICE_PASSWORD=<my password>``

``COPERNICUSMARINE_CREDENTIALS_DIRECTORY``
-------------------------------------------

The toolbox will always look for a credentials file
in the directory set here. By default, the toolbox looks
into the home directory (``$HOME/.copernicusmarine``).
See :ref:`login usage page <login-page>`.

It can be set this way:

- on **UNIX** platforms: ``export COPERNICUSMARINE_CREDENTIALS_DIRECTORY=path/to/directory``
- on **Windows** platforms: ``set COPERNICUSMARINE_CREDENTIALS_DIRECTORY=path\to\directory``

``COPERNICUSMARINE_DISABLE_SSL_CONTEXT``
-----------------------------------------

If set to "True", this will disable the SSL context for the toolbox HTTP calls. Default is "False".
See :ref:`network configuration page about disabling ssl <disable-ssl>`.

It can be set this way:

- on **UNIX** platforms: ``export COPERNICUSMARINE_DISABLE_SSL_CONTEXT=True``
- on **Windows** platforms: ``set COPERNICUSMARINE_DISABLE_SSL_CONTEXT=True``

``COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH``
----------------------------------------------

This will set the path to a custom SSL certificate.
Note that ``COPERNICUSMARINE_DISABLE_SSL_CONTEXT`` takes precedence over ``COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH``.
See :ref:`network configuration page about ssl certificate <ssl-certificate-path>`.

It can be set this way:

- on **UNIX** platforms: ``export COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH=path/to/file.pem``
- on **Windows** platforms: ``set COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH=path\to\file.pem``

``COPERNICUSMARINE_TRUST_ENV``
-------------------------------

If set to "False", this will deactivate the ``trust_env`` value for the ``requests`` library.
"True" by default. See :ref:`network configuration page about trust_env <trust-env>`.

It can be set this way:

- on **UNIX** platforms: ``export COPERNICUSMARINE_TRUST_ENV=False``
- on **Windows** platforms: ``set COPERNICUSMARINE_TRUST_ENV=False``

``COPERNICUSMARINE_HTTPS_TIMEOUT``
----------------------------------

This will set the timeout in seconds for the HTTP calls. Default is ``60``.
See :ref:`network configuration page about timeout <http-connection-timeout-retries>`.

It can be set this way:

- on **UNIX** platforms: ``export COPERNICUSMARINE_HTTPS_TIMEOUT=120``
- on **Windows** platforms: ``set COPERNICUSMARINE_HTTPS_TIMEOUT=120``

``COPERNICUSMARINE_HTTPS_RETRIES``
----------------------------------

This will set the number of retries for the HTTP calls. Default is ``5``.
If set to ``0``, the toolbox won't retry failed HTTP calls.
See :ref:`network configuration page about retries <http-connection-timeout-retries>`.

It can be set this way:

- on **UNIX** platforms: ``export COPERNICUSMARINE_HTTPS_RETRIES=5``
- on **Windows** platforms: ``set COPERNICUSMARINE_HTTPS_RETRIES=5``

``PROXY_HTTPS`` and ``PROXY_HTTP``
-----------------------------------

These allow you to pass a proxy to the toolbox.
See :ref:`network configuration page about proxy <http-proxy>`.

It can be set this way:

- on **UNIX** platforms: ``export PROXY_HTTPS="http://user"``
- on **Windows** platforms: ``set PROXY_HTTPS="http://user"``
