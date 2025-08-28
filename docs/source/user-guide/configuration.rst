.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

Configuration
=============

Let's look at how we can configure DataFusion. When creating a :py:class:`~datafusion.context.SessionContext`, you can pass in
a :py:class:`~datafusion.context.SessionConfig` and :py:class:`~datafusion.context.RuntimeEnvBuilder` object. These two cover a wide range of options.

.. code-block:: python

    from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

    # create a session context with default settings
    ctx = SessionContext()
    print(ctx)

    # create a session context with explicit runtime and config settings
    runtime = RuntimeEnvBuilder().with_disk_manager_os().with_fair_spill_pool(10000000)
    config = (
        SessionConfig()
        .with_create_default_catalog_and_schema(True)
        .with_default_catalog_and_schema("foo", "bar")
        .with_target_partitions(8)
        .with_information_schema(True)
        .with_repartition_joins(False)
        .with_repartition_aggregations(False)
        .with_repartition_windows(False)
        .with_parquet_pruning(False)
        .set("datafusion.execution.parquet.pushdown_filters", "true")
    )
    ctx = SessionContext(config, runtime)
    print(ctx)


.. _target_partitions:

Target partitions and threads
-----------------------------

The :py:meth:`~datafusion.context.SessionConfig.with_target_partitions` method
controls how many partitions DataFusion uses when executing a query. Each
partition is processed on its own thread, so this setting effectively limits
the number of threads that will be scheduled.

For most workloads a good starting value is the number of logical CPU cores on
your machine. You can use :func:`os.cpu_count` to automatically configure this::

    import os
    config = SessionConfig().with_target_partitions(os.cpu_count())

Choosing a value significantly higher than the available cores can lead to
excessive context switching without performance gains, while a much lower value
may underutilize the machine.


You can read more about available :py:class:`~datafusion.context.SessionConfig` options in the `rust DataFusion Configuration guide <https://arrow.apache.org/datafusion/user-guide/configs.html>`_,
and about :code:`RuntimeEnvBuilder` options in the rust `online API documentation <https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeEnvBuilder.html>`_.
