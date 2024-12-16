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

New Copernius Marine authentication system
-------------------------------------------

A new Copernius Marine authentication system will be released in the following months after the release of the Copernicus Marine Toolbox version 2.0.0.
From 2.0.0, the toolbox should be able to handle both the old and the new authentication systems.

If you are blocking some domains, you will need to authorize the domain ``auth.marine.copernicus.eu`` to be able to connect when the old system is decomissioned.

.. note::
    One of limitation of the old system is that it goes through HTTP (through redirections) and not HTTPS. The new system will use HTTPS only.

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
