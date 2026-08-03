# Saves a data frame into a dataset store

Saves a data frame into a dataset store

## Usage

``` r
save_data_frame(
  data,
  dataset,
  name = "data",
  tag = "",
  n_preview = 100,
  save_dir,
  partitioning = NULL
)
```

## Arguments

- data:

  long observations x variables frame to save

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

- partitioning:

  columns to partition the parquet dataset by

## Value

a list with the stored preview and the file location (invisibly)
