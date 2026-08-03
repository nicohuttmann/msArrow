# Loads saved objects from one or several store directories

Loads saved objects from one or several store directories

## Usage

``` r
load_objects(
  dir = "",
  objects = c("Analysis", "Datasets", "Info"),
  exclude = NULL,
  assign = T,
  silent = F
)
```

## Arguments

- dir:

  folder or folders to read

- objects:

  names of the top-level objects to read

- exclude:

  pattern of entries to return as a path instead of reading

- assign:

  assign the objects into the global environment

- silent:

  Should messages be suppressed?

## Value

the list of read objects (invisibly)
