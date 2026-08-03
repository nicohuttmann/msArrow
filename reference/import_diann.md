# Imports a DIA-NN report into a dataset store

Imports a DIA-NN report into a dataset store

## Usage

``` r
import_diann(
  file = "report.parquet",
  name = "Precursors",
  filter_by = Proteotypic == 1 & Decoy == 0,
  observation_names = list(pattern = ".+"),
  variables_data = "default",
  data_frames = "default",
  preview_precursors = c("all", "top100"),
  preview_format = c("wide_obs", "wide_vars", "long"),
  save_dir,
  partition_by_run = F,
  silent = F
)
```

## Arguments

- file:

  path to a DIA-NN report (parquet or tsv)

- name:

  name of the dataset to create

- filter_by:

  expression used to filter the report rows

- observation_names:

  list describing how run names become observations

- variables_data:

  name of a default column set, or column names

- data_frames:

  name of a default quantity set, or column names

- preview_precursors:

  how many precursors to keep in the preview

- preview_format:

  shape of the preview ("wide_obs", "wide_vars", "long")

- save_dir:

  folder the dataset store is written to

- partition_by_run:

  partition the saved data frames by run

- silent:

  Should messages be suppressed?

## Value

TRUE (invisibly); the dataset is written to \<save_dir\> and registered
in Datasets/.Datasets
