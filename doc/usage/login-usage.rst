.. _login-page:

==================
Command ``login``
==================

The ``login`` command creates a configuration file called ``.copernicusmarine-credentials`` that grants access to all Copernicus Marine Data Store services. By default, this file is saved in the user's home directory.

**Example:**

.. code-block:: bash

    > copernicusmarine login
    username : johndoe
    password :
    INFO - Configuration files stored in /Users/foo/.copernicusmarine

If the ``.copernicusmarine-credentials`` file already exists, the system will ask for confirmation before overwriting it. You can also use option ``–-force-overwrite`` to skip confirmation.

Access points migration and evolution
-------------------------------------

If you still have configurations for legacy services (for example, files like ``~/motuclient/motuclient-python.ini``, ``~/.netrc``, or ``~/_netrc`` in your home directory),
these will automatically be recognized by the ``get`` and ``subset`` commands without needing to run the ``login`` command.

If your configuration files are stored in a different directory, you can point directly to the files by using the ``--credentials-file`` option when running the ``get`` or ``subset`` commands.

.. warning::
    The use of motuclient file is deprecated and will be removed in the future.
    Also, the hosts ``nrt.cmems-du.eu`` and ``my.cmems-du.eu`` are deprecated for 'netrc' files and will be removed in the future.
    Please use the new login system and the new host: ``auth.marine.copernicus.eu``.


About ``--check-credentials-valid`` option
-------------------------------------------

The ``--check-credentials-valid`` option allows you to check if the credentials are valid.
It can be useful if you want to check if the credentials are valid before running a long command.

In the Python interface, the function ``check_credentials_valid`` returns a boolean: ``True`` if the credentials are valid, ``False`` otherwise.

In the command line interface, the command will return an exit code of 0 if the credentials are valid, 1 otherwise.

It checks the credentials by trying this order and return if some credentials are found:

1. If ``username`` and ``password`` are provided, it will check them.
2. If ``COPERNICUSMARINE_SERVICE_USERNAME`` and ``COPERNICUSMARINE_SERVICE_PASSWORD`` are set in the environment, it will check them.
3. If the credentials are stored in a configuration file, it will check them.

**Example:**

.. code-block:: bash

    > copernicusmarine login --check-credentials-valid --log-level DEBUG --credentials-file someplace/.copernicusmarine-credentials
    INFO - Checking if credentials are valid.
    INFO - Valid credentials from configuration file.

.. note::
    The ``--check-credentials-valid`` will ignore inputed credentials directory ``--configuration-file-directory``.
    Please use the ``--credentials-file`` option to specify the configuration file to check. This is in order to
    have a check as close as possible to the ``get`` and ``subset`` commands.

About credentials file storage
-------------------------------------------

By default, the credentials file is stored in the user's home directory under the name ``.copernicusmarine-credentials``.
You can specify a different location using the ``--credentials-file`` option when running the ``login`` command.

The credentials file contains the username and password for the Copernicus Marine service and should be kept secure.

It is recommended to set appropriate file permissions to restrict access to the credentials file, for example:

.. code-block:: bash

    chmod 600 ~/.copernicusmarine-credentials

In case you don't want to store your credentials, you can provide them directly using the ``--username`` and ``--password`` options when running any authenticated command.

The python interface allows you to prevent the application from storing your credentials in a configuration file by setting the ``save`` parameter to ``False`` when calling the ``login`` function. This way, the credentials will only be kept in memory for the duration of the Python process and you will be able to chain multiple commands without having to specify them again.

**Example**

.. code-block:: python

    from copernicusmarine.python_interface.login import login
    from copernicusmarine.python_interface.subset import subset

    if login(username="your_username", password="your_password", save=False):
        subset(dataset_id="cmems_obs-oc_atl_bgc-pp_nrt_l4-multi-1km_P1M", x=10, X=-40)


When using the command line interface, the way to achieve this is to set the `COPERNICUSMARINE_SERVICE_USERNAME`and `COPERNICUSMARINE_SERVICE_PASSWORD` environment variables before running any command that requires authentication.

**Example**

.. code-block:: bash

    export COPERNICUSMARINE_SERVICE_USERNAME="your_username"
    export COPERNICUSMARINE_SERVICE_PASSWORD="your_password"
    copernicusmarine subset -i cmems_obs-oc_atl_bgc-pp_nrt_l4-multi-1km_P1M -x 10 -X -40

The credentials won't be stored in a configuration file when using environment variables for authentication. They will only be available for the duration of the current shell session and will be cleared if the terminal window is closed or by unsetting the environment variables using the ``unset`` command, for example:

.. code-block:: bash

    unset COPERNICUSMARINE_SERVICE_USERNAME
    unset COPERNICUSMARINE_SERVICE_PASSWORD
