# Lories Software Framework

```{image} _images/lories-logo.svg
:alt: Lories Logo
:width: 256px
:align: center
```

<div align="center">
  <h1 align="center">Lories</h1>
  <p align="center">
    Lories is an open-source software package for <em><b>Lo</b>cal <b>R</b>esource <b>I</b>ntegration & <b>E</b>xecution <b>S</b>ystems</em> <br>powered by ISC Konstanz e.V.  
  </p>
</div>

Lories is a Python framework for acquiring, processing, and storing time-series data from heterogeneous
sources such as sensors, industrial devices, APIs, and databases. It provides a unified configuration
model where **components** represent logical units (e.g. a weather station or PV system),
**connectors** handle the communication with external systems (Modbus, MQTT, SQL, CSV, ...),
and **channels** carry individual data points through the pipeline. Configuration is driven by
TOML files, making it easy to set up and replicate multi-system deployments without writing code.

```{toctree}
---
hidden:
maxdepth: 1
---
concept/index
connectors/index
code/index

contributing/index
contact
```
