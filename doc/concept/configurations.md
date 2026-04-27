# Configurations

To configure a *lories* project to run, several configuration files may be edited, to customize teh run setup first.  
Entry-point to all configurations is the `settings.conf`, which will be searched for in the local run directory
`./conf/` by default. This configuration directory may be specified with the help of command line arguments when *lories*
is executed, e.g. for a commonly used Linux structure:

```bash
lories --conf-dir=/etc/lories
```

All configuration files are expected to be in the [TOML](https://toml.io/en/) config file format, that aims to be a
minimal configuration file format that's easy to read due to obvious semantics.  
TOML is designed to map unambiguously to a hash table.

The settings allow the more detailed setup of the project directory structure in the `[directories]` section:

```toml
[directories]
# The directory of necessary library files
lib_dir = "/var/lib/lories/"

# The directory for temporary files
tmp_dir = "/var/tmp/lories/"

# The writable directory where configurations that may change during runtime
# can be found or evaluation results will be stored in
data_dir = "/var/opt/lories/"
```


## Systems

Most *lories* projects aggregate several components as a holistic system, to leverage symbiotic effects and added value.  
Entry-point to each system is a `system.conf` file, that will be searched for in the specified `data_dir` by default.
If no data directory is specified, the `conf_dir` will be used.

*Lories* can be configured to run one or several systems at once and can be configured to make a copy of all
configurations to the specified data directory, to allow reproducibility while experimenting with simulations.  
These configurations can be made in the `settings.conf`:

```toml
[systems]
# Specify if several system configurations can be found in the data directory and
# should be scanned for.
#copy = false
scan = true

# If scanning for systems, the systems may be configured to be flat, expecting
# config files to be placed in the systems root directory
flat = true
```

 - `scan = true` configures *lories* to scan for systems in the specified `data_dir`. Any directory in the data
    directory that contains a `./conf/system.conf`, will be run.  
    This allows, among other things, large batch
    simulations.
    ```
    lories
    │
    └───conf
    │   │   settings.conf
    │  
    └───data
    │   │  
    │   └───system_1
    │   │   │  
    │   │   └───conf
    │   │       │   system.conf
    │   │       │   ...
    │   │  
    │   └───system_2
    │       │  
    │       └───conf
    │           │   system.conf
    │           │   ...
    ```
 - `copy = true` will copy all configuration files from the specified `conf_dir`, to a newly created
    directory inside the specified `data_dir`.  
    The directory will be named by the systems identifying key, "system_1" and "system_2" in the example above.
 - `flat = true` allows batch simulations to be clearer to read, by changing the systems directory to contain the
    `system.conf` directly, instead of in a `conf` folder inside.  
    This makes most sense for systems with configured databases and no other local files than the configuration files.
    ```
    lories
    │
    └───conf
    │   │   settings.conf
    │  
    └───data
    │   │  
    │   └───system1
    │       │   system.conf
    │       │   ...
    ```

The system's most important purpose as an entry-point is the identification of a system.  
Each System, is supposed to define a `name`, which will be displayed with any potential evaluation result.  
Optionally, a `key` may be specified for complicated system names, otherwise it will be derived from the name,
by simplifying the name, e.g. by removing special characters.

A `system.conf` file may be quite short:

```toml
# The name of the system
key = "isc"
name = "ISC Konstanz e.V."
```


### Location

Optionally (but commonly), a system is defined by its geographic location, which may be specified in the `[location]`
section:

```toml
[location]
# Geographic location of the system
latitude = 47.67170903328112
longitude = 9.15176162866819
#altitude = <alt>

timezone = "Europe/Berlin"
```

All sections of all configuration files may be refined in the linux-common directory override structure:

A configuration file can expect files in a `<filename>.d` directory, to override values of the file.  
For example here, the system's "Europe/Berlin" timezone will be overridden with the "CET" (Central European Time)
timezone, without pesky daylight-savings in an additional `system.d/location.conf`:

```toml
altitude = 398.4
timezone = "CET"
```


## Components

Each system is intended to contain several components, which can be instanced dynamically and modular via configuration.

*Lories* and all dependant projects provide `Component` classes, providing diverse functionalities. Each component can be
instanced several times, by defining a configuration section or file for each instance.  
A "**test**" component class `MyTestComponent` with the `key = test_1` can be instanced in several ways:

```python
from lories.components import Component, register_component_type


@register_component_type
class MyTestComponent(Component):
    TYPE = "test"
```

 - Add a file to the system's configuration directory, where the filename starts with the *component type* string
   `test_1.conf`:
    ```toml
    name = "Test 1"
    ```
 - Add a file to the system's configuration directory with any name, e.g. `example_1.conf`,
   that specifies the `key` and `type`:
    ```toml
    type = "test"

    key = "test_1"
    name = "Test 1"
    ```
 - Add a specific section to the `system.conf` file, that specifies the `type`:
    ```toml
    [components.test_1]
    type = "test"
    name = "Test 1"
    ```

If any clashes between dedicated configurations of the custom component and the framework-specific configurations occurs,
the `type`, `key` and `name` configs may be specified in a separate `[component]` section for this purpose.


### Channels

This section contains (or will contain) detailed information on the configuration of channels.


## Connectors

This section contains (or will contain) a general overview of the configuration of connectors.

A more detailed overview of all connector specific configurations can be found in the
dedicated [Connectors section](../connectors/index.md).
