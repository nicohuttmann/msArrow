# Writes data to one or several files

`write_data()` decides whether to save the file in the .parquet format
if possible or as an .Rds file and returns the final file path. If no
file name is given, a temporary file will be saved. Depending on the
file and arguments
[`arrow::write_parquet()`](https://arrow.apache.org/docs/r/reference/write_parquet.html),
[`arrow::write_dataset()`](https://arrow.apache.org/docs/r/reference/write_dataset.html)
or R's native [`saveRDS()`](https://rdrr.io/r/base/readRDS.html) will be
used. If redo = F, the function checks if the computation can be
skipped.

## Usage

``` r
write_data(
  x,
  file,
  dir,
  type,
  redo = T,
  return_path = T,
  list_as_folders = T,
  clean_memory = F,
  silent = F,
  partitioning = NULL,
  ...
)
```

## Arguments

- x:

  data to be saved

- file:

  file name (temporary file if not given; file ending will be determined
  automatically)

- dir:

  (optional) folder name if easier to specify separate from file name

- type:

  (optional) one or several file types to write ("parquet", "rds",
  "tsv", "csv" or "txt"); the path of the first type is returned

- redo:

  Should the computation be skipped, if file of given name already
  exists? File name will still be returned.

- return_path:

  Should the saved file path be return or the file itself?

- list_as_folders:

  Should a list be saved as a folder of single files instead of one .Rds
  file?

- clean_memory:

  Should the memory be cleaned with gc()/cleanMem() after writing

- silent:

  Should messages be suppressed?

- partitioning:

  Should the parquet file be split into (parquet only)

- ...:

  additional arguments for the saving function; passed on to every given
  , so format-specific arguments only work with a single type

## Value

file path of the written file, or the path of the first given if several
types were written

## Details

A name that already ends in a recognised type keeps it:
`write_data(x, "data.tsv")` writes `data.tsv`, not `data.tsv.parquet`,
and the ending decides the format. Only the real types count, so
`data_2024.01` and `v1.5` keep their endings and get one appended as
usual.

Giving one or several file types via overrides both the automatic choice
and the ending in the name. Tables can be written as "parquet", "rds",
"tsv", "csv" or "txt"; delimited files are written with
[`arrow::write_csv_arrow()`](https://arrow.apache.org/docs/r/reference/write_csv_arrow.html)
and ".txt" is tab-separated. When several types are given, all files are
written but only the path of the first type is returned, so
`write_data(x, "data_main", "Data", type = c("parquet", "tsv"))` keeps
the pipeline on the parquet file and drops a readable .tsv next to it.

## Examples

``` r
  data_small <- tibble::tibble(a = 1:3, b = letters[1:3]) %>%
    write_data(file = "data_small", 
               dir = tempdir(), 
               type = c("parquet", "tsv"))
#> Overwriting file "/tmp/RtmpMBGaa8/data_small.parquet". Done!
#> Saving file "/tmp/RtmpMBGaa8/data_small.tsv". Done!

  get_data(data_small)
#> # A tibble: 3 × 2
#>       a b    
#>   <int> <chr>
#> 1     1 a    
#> 2     2 b    
#> 3     3 c    
```
