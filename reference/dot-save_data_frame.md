# Writes a data frame of a dataset to disk

Writes a data frame of a dataset to disk

## Usage

``` r
.save_data_frame(
  data_frame,
  data_frame_preview,
  dataset,
  name = "file",
  tag = "",
  n_preview = 100,
  save_dir,
  partitioning = NULL,
  silent = F
)
```

## Arguments

- data_frame:

  long observations x variables frame to save

- data_frame_preview:

  frame stored as the preview (default: first rows)

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

- silent:

  Should messages be suppressed?

## Value

a list with the stored preview and the file location (invisibly)
