# Getting started with msArrow

``` r

library(msArrow)
library(dplyr)
#> 
#> Attaching package: 'dplyr'
#> The following objects are masked from 'package:stats':
#> 
#>     filter, lag
#> The following objects are masked from 'package:base':
#> 
#>     intersect, setdiff, setequal, union
```

## The idea

Mass spectrometry data is large enough that holding several intermediate
versions of it in the R session is the thing that actually runs you out
of memory. msArrow’s answer is that **a variable holds a path, not the
data**.

[`write_data()`](https://nicohuttmann.github.io/msArrow/reference/write_data.md)
writes a table and returns the file it wrote. Every step downstream
re-reads that file.

``` r

dir <- tempfile("msArrow_intro")
dir.create(dir)

data_raw <- tibble::tibble(
  Protein.Group = rep(c("P12345", "Q9Y6K9", "O00305"), each = 4),
  Condition     = rep(c("ctrl", "ctrl", "t8", "t8"), 3),
  Intensity     = c(10.1, 11.4, 15.2, 14.8,
                     5.5,  6.1,  5.9,  6.4,
                    22.0, 21.3, 30.7, 29.9)) %>%
  write_data(file = "data_raw", dir = dir)
#> Saving file "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/data_raw.parquet". Done!

data_raw
#> [1] "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/data_raw.parquet"
```

The variable is a path. To use it, read it:

``` r

get_data(data_raw) %>%
  count(Protein.Group)
#> # A tibble: 3 × 2
#>   Protein.Group     n
#>   <chr>         <int>
#> 1 O00305            4
#> 2 P12345            4
#> 3 Q9Y6K9            4
```

[`get_data()`](https://nicohuttmann.github.io/msArrow/reference/get_data.md)
returns a materialised tibble, so it never needs a trailing
[`collect()`](https://dplyr.tidyverse.org/reference/compute.html).

## Reading lazily

When the file is large and you only need part of it,
[`open_data()`](https://nicohuttmann.github.io/msArrow/reference/open_data.md)
gives an Arrow connection instead. The filter runs before anything is
pulled into R.

``` r

open_data(data_raw) %>%
  filter(Condition == "t8") %>%
  summarise(mean_int = mean(Intensity), .by = Protein.Group) %>%
  collect()
#> # A tibble: 3 × 2
#>   Protein.Group mean_int
#>   <chr>            <dbl>
#> 1 P12345           15   
#> 2 Q9Y6K9            6.15
#> 3 O00305           30.3
```

## Choosing the file type

Left alone,
[`write_data()`](https://nicohuttmann.github.io/msArrow/reference/write_data.md)
decides for itself: parquet when Arrow can represent the object, a
`.parquetlist` folder for a list, `.Rds` otherwise.

The `type` argument overrides that. It takes one or several of
`parquet`, `rds`, `tsv`, `csv` and `txt`.

``` r

data_summary <- get_data(data_raw) %>%
  summarise(mean_int = mean(Intensity),
            n        = n(),
            .by      = c(Protein.Group, Condition)) %>%
  write_data(file = "data_summary", dir = dir, type = c("parquet", "tsv"))
#> Saving file "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/data_summary.parquet". Done!
#> Saving file "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/data_summary.tsv". Done!

list.files(dir, pattern = "data_summary")
#> [1] "data_summary.parquet" "data_summary.tsv"
```

Both files were written. **The returned path is the first type only**:

``` r

basename(data_summary)
#> [1] "data_summary.parquet"
```

That is the point of the ordering. A pipeline stays on the fast parquet
file, while a `.tsv` that a collaborator can open lands beside it
without a second line of code. For a small table that only ever needs to
be human-readable, `type = "tsv"` on its own does the job:

``` r

get_data(data_raw) %>%
  distinct(Protein.Group) %>%
  write_data(file = "protein_list", dir = dir, type = "tsv") %>%
  get_data()
#> Saving file "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/protein_list.tsv". Done!
#> # A tibble: 3 × 1
#>   Protein.Group
#>   <chr>        
#> 1 P12345       
#> 2 Q9Y6K9       
#> 3 O00305
```

`.txt` is written tab-separated, `.csv` comma-separated. Delimited files
are written through Arrow, and values are quoted where needed so a field
containing a tab or comma still round-trips:

``` r

tibble::tibble(Genes = c("A,B", "C\tD"), n = 1:2) %>%
  write_data(file = "tricky", dir = dir, type = "tsv") %>%
  get_data()
#> Saving file "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/tricky.tsv". Done!
#> # A tibble: 2 × 2
#>   Genes      n
#>   <chr>  <int>
#> 1 "A,B"      1
#> 2 "C\tD"     2
```

Delimited formats store flat tables only. Asking for a `.tsv` of
something with a list column is an error rather than a silently mangled
file.

## Skipping work that is already done

[`write_data()`](https://nicohuttmann.github.io/msArrow/reference/write_data.md)
recalculates by default. Pass `redo = FALSE` to return the existing path
instead, when a step is genuinely expensive and you are sure the inputs
have not changed:

``` r

write_data(get_data(data_raw), file = "data_raw", dir = dir, redo = FALSE)
#> Returning location of existing file "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/data_raw.parquet".
#> [1] "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/data_raw.parquet"
```

With several types, the computation is skipped only when *every*
requested file already exists — otherwise the missing one would never be
written.

## Lists become folders

A list is written as a folder of files rather than one large `.Rds`, so
a single element can be read without loading the rest.

``` r

data_split <- get_data(data_raw) %>%
  split(.$Condition) %>%
  write_data(file = "by_condition", dir = dir)
#> Creating folder "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/by_condition.parquetlist". Done!
#> Saving file "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/by_condition.parquetlist/ctrl.parquet". Done!
#> Saving file "/tmp/Rtmp2EuoRA/msArrow_intro1f28316ce6a8/by_condition.parquetlist/t8.parquet". Done!

list.files(data_split)
#> [1] "ctrl.parquet" "t8.parquet"

get_data(data_split)$t8
#> # A tibble: 6 × 3
#>   Protein.Group Condition Intensity
#>   <chr>         <chr>         <dbl>
#> 1 P12345        t8             15.2
#> 2 P12345        t8             14.8
#> 3 Q9Y6K9        t8              5.9
#> 4 Q9Y6K9        t8              6.4
#> 5 O00305        t8             30.7
#> 6 O00305        t8             29.9
```

## Where the files went

Without a `file` name,
[`write_data()`](https://nicohuttmann.github.io/msArrow/reference/write_data.md)
writes into [`tempdir()`](https://rdrr.io/r/base/tempfile.html). These
helpers show what has accumulated and clear it:

``` r

length(tempdir_list())
#> [1] 0
```

## Next steps

For annotated datasets — variables, observations and long
`observations x variables` frames kept together on disk, and DIA-NN
reports imported straight into that layout — see
[`import_diann()`](https://nicohuttmann.github.io/msArrow/reference/import_diann.md),
[`get_data_frame()`](https://nicohuttmann.github.io/msArrow/reference/get_data_frame.md)
and
[`get_variables_data()`](https://nicohuttmann.github.io/msArrow/reference/get_variables_data.md)
in the [reference](https://nicohuttmann.github.io/msArrow/reference/).
