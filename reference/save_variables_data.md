# Adds or updates variables data in a dataset

Adds or updates variables data in a dataset

## Usage

``` r
save_variables_data(
  data,
  columns,
  name = "variables",
  tag = "",
  dataset,
  n_preview = 100,
  save_dir
)
```

## Arguments

- data:

  variables data as tibble or Arrow object

- columns:

  columns of to store (default: all but "variables")

- name:

  name the frame is stored under

- tag:

  suffix appended to the file name

- dataset:

  dataset name

- n_preview:

  number of rows kept in the preview

- save_dir:

  folder the dataset store lives in

## Value

a list with the stored preview and the file location (invisibly)
