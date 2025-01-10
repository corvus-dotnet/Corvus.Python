# corvus-python

This package is an interim home for data-related & python-based utility functionality, particularly focussed on:

* Improving the inner dev loop when working with Azure Synapse Notebooks
* Encapsulating opionated data engineering design concepts (e.g. Medallion Data Lake Architecture)

## Tracing

Helper methods to support Open Telemetry-based tracing are provided in `corvus_python.monitoring.tracing`. These can be applied to code, but will only have any effect if suitable OpenTelemetry configuration is performed in the consuming package.

There are two decorators and four helper methods:

Decorators:
- `start_as_current_span_with_method_name` - applied to a function, this will start a span named for the function that's created when the function is called and lasts for the duration of the function execution.
- `all_methods_start_new_current_span_with_method_name` - applies the `start_as_current_span_with_method_name` to all callable members of the class

Helpers:
- `add_attributes_to_span` adds the specified keyword arguments to the given span as attributes.
- `add_attributes_to_current_span` is similar to `add_attributes_to_span`, but uses the current span rather than requiring it to be passed in.
- `add_kwargs_to_span` adds the specified keys from the kwargs of a function to the given span.
- `add_kwargs_to_current_span` is similar to `add_kwargs_to_span`, but uses the current span rather than requiring it to be passed in.

### Configuration for Application Insights

To use these decorators and methods, you must configure Open Telemetry correctly in your code. To configure traces to be sent to Application Insights, take the following steps:

1. Add the `azure-monitor-opentelemetry` to your project. Assuming your package is being deployed to a Spark cluster, you will also need to deploy this package on the cluster.
2. Use the `configure_azure_monitor` function to set up Open Telemetry:

```python
configure_azure_monitor(
    connection_string="[Your connection string here]",
    resource=Resource.create({
        "service.name": "[Your service name here]",
        "service.instance.id": "[Your service instance Id here]"
    })
```

Store your connection string from an Azure KeyVault and retrieve using `mssparkutils.credentials.getSecretWithLS` or equivalent.

### Writing log messages to App Insights

If your code is already using standard Python logging via the `logging` namespace, the `configure_azure_monitor` method will ensure that these messages are written to App Insights as traces of type `TRACE`, with their parent being the current span at the point they were logged. However, you should prefer writing useful data as attributes of your spans rather than in log messages; this will make it simpler to query them using the Kusto query language in Azure Monitor.

### Recommendations when running in Synapse

These recommendations are intended for the scenario when you are deploying packages into a Spark cluster to be executed from a Synapse notebook, although there will be equivalent implementations for other platforms.

#### Add the Azure Monitor configuration to the notebook which calls your code

Adding the configuration to the notebook rather than inside your package means that it will be possible to apply different Open Telemetry configuration elsewhere - for example, in test runs - or leave it out completely if required to effectively disable tracing.

#### Service name and service instance Id

Use your package name as the service name. This will be accesible in App Insights as the `Role name` in every span that is sent. If applying a filter in the transaction search screen, it is referred to as `Cloud role name`.

Use a suitable correlation Id for the service instance Id. This will be accessible in App Insights as the `Role instance` in every span that is sent. If applying a filter in the transaction search screen, it is referred to as `Cloud role instance`.

When code is executed from a Synapse notebook, it's likely that it will be being invoked from a pipeline. In this case you can set `service.instance.id` to the Pipeline run Id to provide an easy link between pipeline runs in the Synapse monitor. This can either be passed into the notebook as a parameter, or accessed in the notebook using `mssparkutils.runtime.context["pipelinejobid"]`.

#### Span types

The supplied decorators create spans of kind `SpanKind.INTERNAL` - the default. This is intended to indicate that the span represents an internal operation within an application, as opposed to an operations with remote parents or children. This will result in a trace of type `DEPENDENCY` in App Insights (also referred to as `InProc`).

Consider wrapping calls to your package with a manually created span of kind `SpanKind.REQUEST`. This will result in a trace of type `REQUEST` in App Insights, and will make it easier to locate invocations of your package by searching for spans of type `REQUEST` using your correlation Id.

If your package has a small number of entrypoints, you can create the Request span inside your package. Alternatively, you can create the Request span in the notebook which uses your package. The code looks like this:

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

def my_entrypoint():

    with tracer.start_as_current_span(
            "my_entrypoint_name",
            kind=trace.SpanKind.SERVER) as span:

        # Your code here.
```

#### Sensitive data

Never send sensitive data to App Insights. The need to avoid this is why the helper methods do not add all of a functions arguments to span attributes, and why the helper method `add_kwargs_to_span` requires you to explicitly specify the keys to add. If sensitive data is accidentally logged into App Insights, it should be considered a data breach. 