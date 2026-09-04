# Returns the file type a name already carries

Only the recognised endings count, so a file called "data_2024.01" or
"v1.5" keeps its ending instead of losing it to `file_path_sans_ext()`.
The match ignores case, so ".TSV" is a tsv.

## Usage

``` r
.type_in_name(file_dir)
```

## Arguments

- file_dir:

  file path

## Value

the type in lower case, or NA if the name carries none
