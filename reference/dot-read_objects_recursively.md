# Reads a folder of files back into a nested list

Reads a folder of files back into a nested list

## Usage

``` r
.read_objects_recursively(
  name,
  dir,
  exclude = NULL,
  silent = F,
  as_arrow_table = F,
  ...
)
```

## Arguments

- name:

  name of the folder or file to read

- dir:

  folder the object lives in

- exclude:

  pattern of entries to return as a path instead of reading

- silent:

  Should messages be suppressed?

- as_arrow_table:

  return tibbles or Arrow connections

- ...:

  additional arguments for the reading function

## Value

the nested list rebuilt from the folder tree
