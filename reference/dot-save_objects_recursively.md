# Saves a nested list as a folder of single files

Saves a nested list as a folder of single files

## Usage

``` r
.save_objects_recursively(
  object,
  name,
  dir,
  type = NULL,
  silent = F,
  redo = T,
  list_as_folders = T,
  clean_memory = F,
  partitioning = NULL,
  ...
)
```

## Arguments

- object:

  list or single object to be saved

- name:

  name of the folder or file

- dir:

  folder the object is saved in

- type:

  (optional) file type used for every table in the list

- silent:

  Should messages be suppressed?

- redo:

  Should existing files be written again?

- list_as_folders:

  Should nested lists become nested folders?

- clean_memory:

  Should the memory be cleaned with gc()/cleanMem() afer writing

- partitioning:

  Should the parquet files be split into

- ...:

  additional arguments for the saving function

## Value

TRUE (invisibly)
