# Settings

```{code-block} toml
:caption: settings.conf

[directories]
# The directory of necessary library files
lib_dir = "/var/lib/penguin/"

# The directory for temporary files
tmp_dir = "/var/tmp/penguin/"

# The writable directory where configurations may be found or results will be stored in
data_dir = "/var/opt/penguin/"
```


# Systems

```{code-block} toml
:caption: settings.conf

[systems]
# Specify if several system configurations can be found in the data directory and
# should be scanned for.
#copy = false
scan = true

# If scanning for systems, the systems may be configured to be flat, expecting 
# config files to be placed in the systems root directory
flat = true
```


```{code-block} toml
:caption: system.conf

# The name of the system
key = "isc"
name = "ISC Konstanz e.V."
```


## Location

```{code-block} toml
:caption: system.conf

[location]
# Geographic location of the system
latitude = 47.67170903328112
longitude = 9.15176162866819
#altitude = <alt>
```

```{code-block} toml
:caption: system.d/location.conf

altitude = 398.4
timezone = "Europe/Berlin"
```
