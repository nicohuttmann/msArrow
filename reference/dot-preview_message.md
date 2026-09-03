# Reports what a preview is showing

Kept separate from building the preview so that
[`view_data()`](https://nicohuttmann.github.io/msArrow/reference/view_data.md)
can open the viewer first and report afterwards.

## Usage

``` r
.preview_message(previews, n_rows, total_sets = NULL)
```

## Arguments

- previews:

  a preview tibble, or a named list of them

- n_rows:

  number of rows that was asked for

- total_sets:

  number of datasets the file holds, used for the hint

## Value

nothing; the counts are printed
