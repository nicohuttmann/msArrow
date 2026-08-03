# Adds or updates observations data in a dataset

Adds or updates observations data in a dataset

## Usage

``` r
save_observations_data(
  data,
  columns,
  dataset,
  name = "observations",
  tag = "",
  n_preview = 100,
  save_dir
)
```

## Arguments

- data:

  observations data as tibble or Arrow object

- columns:

  columns of to store (default: all but "observations")

- dataset:

  dataset name

- name:

  name the frame is stored under

- tag:

  suffix appended to the file name

- n_preview:

  number of rows kept in the preview

- save_dir:

  folder the dataset store lives in

## Value

a list with the stored preview and the file location (invisibly)
