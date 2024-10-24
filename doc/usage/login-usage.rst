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

If the ``.copernicusmarine-credentials`` file already exists, the system will ask for confirmation before overwriting it. You can also use option ``–-overwrite`` or ``--overwrite-configuration-file`` to skip confirmation.

You can also use the ``--skip-if-user-logged-in`` option to prevent overwriting the configuration file if the user is already logged in.

New Copernius Marine authentication system
-------------------------------------------

A new Copernius Marine authentication system will be released in the following months after the release of the Copernicus Marine toolbox version 2.0.0.
From 2.0.0, the toolbox should be able to handle both the old and the new authentication systems.

If you are blocking some domains, you will need to authorize the domain ``auth.marine.copernicus.eu`` to be able to connect when the old system is decomissioned.

.. note::
    One of limitation of the old system is that it goes through HTTP (through redirections) and not HTTPS. The new system will use HTTPS only.

Access points migration and evolution
-------------------------------------

If you still have configurations for legacy services (for example, files like ``~/motuclient/motuclient-python.ini``, ``~/.netrc``, or ``~/_netrc`` in your home directory), these will automatically be recognized by the ``get`` and ``subset`` commands without needing to run the ``login`` command.

If your configuration files are stored in a different directory, you can point to them by using the ``--credentials-file`` option when running the ``get`` or ``subset`` commands.
