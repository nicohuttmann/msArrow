# Writes a single file of a given file type

Writes a single file of a given file type

## Usage

``` r
.write_data_file(x, file_dir, type, silent = F, partitioning = NULL, ...)
```

## Arguments

- x:

  data to be saved

- file_dir:

  final file path including the file type ending

- type:

  file type to write

- silent:

  Should messages be suppressed?

- partitioning:

  partitioning columns (parquet only)

- ...:

  additional arguments for the saving function

## Value

file path of the written file (invisibly)
