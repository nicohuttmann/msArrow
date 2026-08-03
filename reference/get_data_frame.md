# Assemble data frame from dataset

Assemble data frame from dataset

## Usage

``` r
get_data_frame(
  which,
  variables,
  observations,
  dataset,
  output.type = "tibble",
  ...
)
```

## Arguments

- which:

  specific name of data type

- variables:

  selected variables

- observations:

  selected observations

- dataset:

  dataset name

- output.type:

  data type (default = "tibble", "data.frame", "matrix")

- ...:

  additional arguments for open_data()
