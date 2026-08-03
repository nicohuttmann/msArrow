# Writes the variables data of a dataset to disk

Writes the variables data of a dataset to disk

## Usage

``` r
.save_variables_data(
  variables_data_frame,
  variables_data_frame_preview,
  dataset,
  name = "variables",
  tag = "",
  n_preview = 100,
  save_dir
)
```

## Arguments

- variables_data_frame:

  variables data to save

- variables_data_frame_preview:

  frame stored as the preview

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
