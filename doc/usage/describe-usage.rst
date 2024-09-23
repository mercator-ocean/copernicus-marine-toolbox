Command ``describe``
=====================

The ``describe`` command retrieves metadata information about all products and datasets, displaying it as a JSON output.

**Example:**

.. code-block:: bash

    copernicusmarine describe --include-datasets

To save the JSON output to a file, you can use the following command:

.. code-block:: bash

    copernicusmarine describe --include-datasets > all_datasets_copernicusmarine.json
