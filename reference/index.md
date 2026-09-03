# Package index

## Reading and writing data

The core contract: a variable holds a *path*, not the data.
[`write_data()`](https://nicohuttmann.github.io/msArrow/reference/write_data.md)
returns the file it wrote, and every downstream step re-reads it. This
is deliberate memory management for large MS data.

- [`write_data()`](https://nicohuttmann.github.io/msArrow/reference/write_data.md)
  : Writes data to one or several files
- [`get_data()`](https://nicohuttmann.github.io/msArrow/reference/get_data.md)
  : Loads data from a file name or returns if already in R
- [`open_data()`](https://nicohuttmann.github.io/msArrow/reference/open_data.md)
  : Loads data from a file name
- [`get_data_m()`](https://nicohuttmann.github.io/msArrow/reference/get_data_m.md)
  : Combines a get_data(), map() and bind_rows()

## Looking at a file

Read the first rows of something large without loading it. Both report
how many rows the file actually holds, which costs nothing to look up.

- [`preview_data()`](https://nicohuttmann.github.io/msArrow/reference/preview_data.md)
  : Reads the first rows of a file without loading the rest
- [`view_data()`](https://nicohuttmann.github.io/msArrow/reference/view_data.md)
  : Opens the first rows of a file in the data viewer

## Temporary files

[`write_data()`](https://nicohuttmann.github.io/msArrow/reference/write_data.md)
without a file name writes into
[`tempdir()`](https://rdrr.io/r/base/tempfile.html). These list, measure
and clear what accumulated there.

- [`tempdir_list()`](https://nicohuttmann.github.io/msArrow/reference/tempdir_list.md)
  : List all temporary files saved by write_data() when
- [`tempdir_remove()`](https://nicohuttmann.github.io/msArrow/reference/tempdir_remove.md)
  : Removing all temporary files saved by write_data() when
- [`tempdir_size()`](https://nicohuttmann.github.io/msArrow/reference/tempdir_size.md)
  : List size of all temporary files saved by write_data() when

## Nested lists on disk

A list is written as a folder of files (`.parquetlist`) rather than one
large `.Rds`, so single elements can be read without loading the rest.

- [`load_objects()`](https://nicohuttmann.github.io/msArrow/reference/load_objects.md)
  : Loads saved objects from one or several store directories
- [`.save_objects_recursively()`](https://nicohuttmann.github.io/msArrow/reference/dot-save_objects_recursively.md)
  : Saves a nested list as a folder of single files
- [`.read_objects_recursively()`](https://nicohuttmann.github.io/msArrow/reference/dot-read_objects_recursively.md)
  : Reads a folder of files back into a nested list

## Utilities

- [`cleanMem()`](https://nicohuttmann.github.io/msArrow/reference/cleanMem.md)
  : Garbage collection function ruthlessly copied from
  https://stackoverflow.com/a/1467334
- [`` `%>%` ``](https://nicohuttmann.github.io/msArrow/reference/pipe.md)
  : Pipe operator
