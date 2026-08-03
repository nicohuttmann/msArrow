# Writes the observations data of a dataset to disk

Writes the observations data of a dataset to disk

## Usage

``` r
.save_observations_data(
  observations_data_frame,
  observations_data_frame_preview,
  name = "observations",
  tag = "",
  dataset,
  n_preview = 100,
  save_dir,
  silent = F
)
```

## Arguments

- observations_data_frame:

  observations data to save

- observations_data_frame_preview:

  frame stored as the preview

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

- silent:

  Should messages be suppressed?

## Value

a list with the stored preview and the file location (invisibly)
