.types_allowed <- c("parquet", "rds", "tsv", "csv", "txt")


#' Checks file types given to write_data()
#'
#' @param type one or several file types
#'
#' @returns lowercase vector of unique, valid file types
#' @keywords internal
.check_data_type <- function(type) {
  
  type <- unique(tolower(as.character(type)))
  
  if (length(type) == 0)
    stop("Please provide at least one file <type>.", call. = F)
  
  if (any(!type %in% .types_allowed))
    stop(paste0('Unknown file <type> "', 
                paste(setdiff(type, .types_allowed), collapse = '", "'), 
                '". Use one or several of "', 
                paste(.types_allowed, collapse = '", "'), 
                '".'), 
         call. = F)
  
  return(type)
  
}


#' Delimiter used for a delimited file type
#'
#' @param type file type ("tsv", "csv" or "txt")
#'
#' @returns single character delimiter (NA for non-delimited types)
#' @keywords internal
.data_type_delim <- function(type) {
  unname(c(parquet = NA, 
           rds = NA, 
           tsv = "\t", 
           csv = ",", 
           txt = "\t")[type])
}


#' Removes a known file type ending from a file path
#'
#' @param file_dir file path
#'
#' @returns file path without a trailing known file type ending
#' @keywords internal
.trim_data_type <- function(file_dir) {
  ifelse(tolower(tools::file_ext(file_dir)) %in% .types_allowed, 
         tools::file_path_sans_ext(file_dir), 
         file_dir)
}


#' Builds the file paths for one or several file types
#'
#' @param file_dir file path without or with file type ending
#' @param type one or several file types
#'
#' @returns file paths, one per given type
#' @keywords internal
.add_data_type <- function(file_dir, type) {
  paste0(.trim_data_type(file_dir), 
         ".", 
         ifelse(type == "rds", "Rds", type))
}


#' Writes a single file of a given file type
#'
#' @param x data to be saved
#' @param file_dir final file path including the file type ending
#' @param type file type to write
#' @param silent Should messages be suppressed?
#' @param partitioning partitioning columns (parquet only)
#' @param ... additional arguments for the saving function
#'
#' @returns file path of the written file (invisibly)
#' @keywords internal
.write_data_file <- function(x, 
                             file_dir, 
                             type, 
                             silent = F, 
                             partitioning = NULL, 
                             ...) {
  
  if (!silent) {
    if (!file.exists(file_dir)) {
      cat(paste0('Saving file "', 
                 file_dir, 
                 '".'))
    } else {
      cat(paste0('Overwriting file "', 
                 file_dir, 
                 '".'))
    }
  }
  
  # parquet file or partitioned dataset
  if (type == "parquet") {
    
    if (is.null(partitioning))
      arrow::write_parquet(x = x, 
                           sink = file_dir, 
                           ...)
    
    else
      arrow::write_dataset(dataset = x, 
                           path = file_dir, 
                           partitioning = partitioning, 
                           existing_data_behavior = "delete_matching", 
                           ...)
    
    # delimited file
  } else if (type %in% c("tsv", "csv", "txt")) {
    
    write_options <- arrow::CsvWriteOptions$create(include_header = T, 
                                                   delimiter = .data_type_delim(type))
    
    written <- tryCatch(arrow::write_csv_arrow(x = x, 
                                               sink = file_dir, 
                                               write_options = write_options, 
                                               ...), 
                        error = function(e) e)
    
    if (inherits(written, "error"))
      stop(paste0('Data cannot be written as a .', 
                  type, 
                  ' file: ', 
                  conditionMessage(written), 
                  '\n  Delimited files store flat tables only - use ', 
                  'type = "parquet" or type = "rds" for these data.'), 
           call. = F)
    
    if (!is.null(partitioning) && !silent)
      cat(paste0(" <partitioning> ignored for .", type, " files."))
    
    # Rds file
  } else {
    
    saveRDS(x, file_dir, ...)
    
  }
  
  if (!silent) cat(" Done!\n")
  
  return(invisible(file_dir))
  
}


#' Writes data to one or several files
#'
#' `write_data()` decides whether to save the file in the .parquet format if
#' possible or as an .Rds file and returns the final file path. If no file 
#' name is given, a temporary file will be saved. Depending on the file and 
#' arguments `arrow::write_parquet()`, `arrow::write_dataset()` or R's native 
#' `saveRDS()` will be used. If redo = F, the function checks if the computation can be skipped.
#'
#' Giving one or several file types via <type> overrides this automatic choice.
#' Tables can be written as "parquet", "rds", "tsv", "csv" or "txt"; delimited
#' files are written with `arrow::write_csv_arrow()` and ".txt" is
#' tab-separated. When several types are given, all files are written but only
#' the path of the first type is returned, so
#' `write_data(x, "data_main", "Data", type = c("parquet", "tsv"))` keeps the
#' pipeline on the parquet file and drops a readable .tsv next to it.
#'
#' @param x data to be saved
#' @param file file name (temporary file if not given; file ending will be 
#' determined automatically)
#' @param dir (optional) folder name if easier to specify separate from file
#' name
#' @param type (optional) one or several file types to write ("parquet", "rds", 
#' "tsv", "csv" or "txt"); the path of the first type is returned
#' @param redo Should the computation be skipped, if file of given name already
#' exists? File name will still be returned.
#' @param return_path Should the saved file path be return or the file itself?
#' @param list_as_folders Should a list be saved as a folder of single files
#' instead of one .Rds file?
#' @param clean_memory Should the memory be cleaned with gc()/cleanMem() after
#' writing
#' @param silent Should messages be suppressed?
#' @param partitioning Should the parquet file be split into (parquet only)
#' @param ... additional arguments for the saving function; passed on to every
#' given <type>, so format-specific arguments only work with a single type
#' @returns file path of the written file, or the path of the first given
#' <type> if several types were written
#' @export
#'
#' @examples
#'   data_small <- tibble::tibble(a = 1:3, b = letters[1:3]) %>%
#'     write_data(file = "data_small", 
#'                dir = tempdir(), 
#'                type = c("parquet", "tsv"))
#'
#'   get_data(data_small)
write_data <- function(x, 
                       file, 
                       dir, 
                       type, 
                       redo = T, 
                       return_path = T, 
                       list_as_folders = T, 
                       clean_memory = F, 
                       silent = F, 
                       partitioning = NULL, 
                       ...) {
  
  # 
  if (!hasArg(dir) & !hasArg(file)) {
    file <- tempfile()
    #stop("Please provide the data <x> and a <file>.")
  } else if (!hasArg(file)) {
    stop("Please provide the data <x> and a <file> name when specifying a directory <dir>.")
  }
  
  # Get final file path
  if (hasArg(dir)) file_dir <- file.path(dir, file)
  else file_dir <- file
  
  # Check given file types
  if (hasArg(type)) type <- .check_data_type(type)
  else type <- NULL
  
  
  # Check if x should be run or simply save the location
  if (!redo) {
    
    if (is.null(type)) files_expected <- paste0(.trim_data_type(file_dir), 
                                                c(".parquet", ".Rds"))
    else files_expected <- .add_data_type(file_dir, type)
    
    files_found <- purrr::map_lgl(files_expected %>%
                                    setNames(., .), 
                                  file.exists)
    
    # every given type must exist before the computation is skipped
    skip <- if (is.null(type)) any(files_found) else all(files_found)
    
    if (skip) {
      
      file_return <- names(which(files_found))[1]
      
      if (!silent) cat(paste0('Returning location of existing file "', 
                              file_return, 
                              '".\n'))
      
      return(file_return)
      
    }
    
  }
  
  
  # Check if any data is given 
  if (!hasArg(x)) stop("No data <x> given.")
  
  # Create directory if does not exist 
  if (!dir.exists(dirname(file_dir))) 
    dir.create(dirname(file_dir), recursive = T)
  
  # Write every given file type
  if (!is.null(type)) {
    
    # Save list as parquetlist
    if (class(x)[1] == "list" && list_as_folders) {
      
      if (length(type) > 1)
        stop(paste0('Only one <type> can be given for a list, as the files ', 
                    'inside a .parquetlist folder would share one name.'), 
             call. = F)
      
      .save_objects_recursively(object = x, 
                                name = paste0(file, ".parquetlist"), 
                                dir = dir, 
                                type = type, 
                                silent = silent, 
                                redo = redo, 
                                list_as_folders = list_as_folders, 
                                clean_memory = clean_memory, 
                                partitioning = partitioning, 
                                ...)
      
      file_dir <- paste0(file_dir, ".parquetlist")
      
      # Save table as every given file type
    } else {
      
      files_written <- .add_data_type(file_dir, type)
      
      for (i in seq_along(type))
        .write_data_file(x = x, 
                         file_dir = files_written[i], 
                         type = type[i], 
                         silent = silent, 
                         partitioning = partitioning, 
                         ...)
      
      # only the first given type is returned
      file_dir <- files_written[1]
      
    }
    
    # Choose the file type automatically
  } else {
    
    # Save as parquet file or dataset
    if (((tibble::is_tibble(x) ||
          is.data.frame(x) ||
          is.matrix(x)) &&
         length(tryCatch(arrow::infer_type(x), error = function(e) NULL)) > 0) ||
        any(stringr::str_detect(class(x), "(A|a)rrow"))) {
      
      if (!stringr::str_detect(tolower(file_dir), "\\.parquet$"))
        file_dir <- paste0(file_dir, ".parquet")
      
      .write_data_file(x = x, 
                       file_dir = file_dir, 
                       type = "parquet", 
                       silent = silent, 
                       partitioning = partitioning, 
                       ...)
      
      # Save list as parquetlist
    } else if (class(x)[1] == "list" && list_as_folders) {
      
      .save_objects_recursively(object = x, 
                                name = paste0(file, ".parquetlist"), 
                                dir = dir, 
                                silent = silent, 
                                redo = redo, 
                                list_as_folders = list_as_folders, 
                                clean_memory = clean_memory, 
                                partitioning = partitioning, 
                                ...)
      
      file_dir <- paste0(file_dir, ".parquetlist")
      
      # Save as Rds file
    } else {
      
      if (!stringr::str_detect(tolower(file_dir), "\\.rds$"))
        file_dir <- paste0(file_dir, ".Rds")
      
      .write_data_file(x = x, 
                       file_dir = file_dir, 
                       type = "rds", 
                       silent = silent, 
                       ...)
      
    }
    
  }
  
  if (!isFALSE(clean_memory)) cleanMem(clean_memory)
  
  if (return_path)
    return(file_dir)
  else 
    return(x)
  
}


#' Loads data from a file name or returns if already in R
#'
#' @param file file name 
#' @param fallback other file name or alternative way to provide the input - 
#' useful if file is an R object and fallback is a hardcoded string 
#' @param recursive Should data be recursively loaded?
#' @param credit how many recursive steps are allowed 
#' @param as_arrow_table return a tibble or an Arrow connection 
#' @param ... additional arguments
#'
#' @returns a tibble, or an Arrow dataset connection if as_arrow_table = T; the
#' object itself if <file> is not a file path
#' @export
#'
#' @examples
#'   data_small <- tibble::tibble(a = 1:3, b = letters[1:3]) %>%
#'     write_data(file = "data_small", 
#'                dir = tempdir(), 
#'                type = "tsv")
#'
#'   get_data(data_small)
get_data <- function(file, 
                     fallback, 
                     recursive = T, 
                     credit = 10, 
                     as_arrow_table = F, 
                     ...) {
  
  # Check if input is given or if fallback options is given 
  if (missing(file) || tryCatch(is.null(file), 
                                error = function(e) TRUE)) {
    
    if (missing(fallback)) {
      stop("No <file> or <fallback> file argument given.")
    } else {
      file <- fallback
    }
    
  }
  
  if (length(file) > 1) {
    
    return(file)
    
  } else {
    
    if (!is.character(file)) {
      
      if (class(file)[1] == "list") data_object <- purrr::map(file, get_data)
      else data_object <- file
      
    } else if (tolower(tools::file_ext(file)) == "parquetlist") {
      
      data_object <- .read_objects_recursively(name = file, 
                                               silent = T, 
                                               as_arrow_table = as_arrow_table, 
                                               ...)
      
    } else if (tolower(tools::file_ext(file)) == "parquet") {
      
      if (!as_arrow_table) {
        
        data_object <- tibble::as_tibble(arrow::read_parquet(file))
        
      } else {
        
        data_object <- arrow::open_dataset(file, ...)
        
      }
      
    } else if (tolower(tools::file_ext(file)) %in% c("tsv", "csv", "txt")) {
      
      delim <- .data_type_delim(tolower(tools::file_ext(file)))
      
      if (!as_arrow_table)
        data_object <- arrow::read_delim_arrow(file = file, 
                                               delim = delim, 
                                               ...) %>%
          tibble::as_tibble()
      
      else
        data_object <- arrow::open_delim_dataset(sources = file, 
                                                 delim = delim, 
                                                 ...)
      
    } else if (tolower(tools::file_ext(file)) == "rds") {
      
      if (recursive) data_object <- readRDS(file) %>%
          get_data(recursive = credit - 1 > 0, 
                   credit = credit - 1, 
                   as_arrow_table = as_arrow_table)
      
      else data_object <- readRDS(file)
      
      
    } else {
      
      data_object <- file
      
    }
    
  }
  
  return(data_object)
  
}


#' Loads data from a file name 
#'
#' @param file file name 
#' @param fallback other file name or alternative way to provide the input - 
#' useful if file is an R object and fallback is a hardcoded string 
#' @param recursive Should data be recursively loaded?
#' @param credit how many recursive steps are allowed
#' @param ... additional arguments for the opening function
#'
#' @returns an Arrow dataset connection, or the object itself if <file> is not
#' a file path
#' @export
#'
#' @examples
#'   data_small <- tibble::tibble(a = 1:3, b = letters[1:3]) %>%
#'     write_data(file = "data_small", 
#'                dir = tempdir(), 
#'                type = "tsv")
#'
#'   open_data(data_small) %>%
#'     dplyr::filter(a > 1) %>%
#'     dplyr::collect()
open_data <- function(file, 
                      fallback, 
                      recursive = T, 
                      credit = 10, 
                      ...) {
  
  # Check if input is given or if fallback options is given 
  if (missing(file) || tryCatch(is.null(file), 
                                error = function(e) TRUE)) {
    
    if (missing(fallback)) {
      stop("No <file> or <fallback> file argument given.")
    } else {
      file <- fallback
    }
    
  }
  
  if (length(file) > 1) {
    
    return(file)
    
  } else {
    
    if (!is.character(file)) {
      
      if (class(file)[1] == "list") data_object <- purrr::map(file, get_data)
      else data_object <- file
      
    }  else if (tolower(tools::file_ext(file)) == "rds") {
      
      if (recursive) data_object <- readRDS(file) %>%
          open_data(recursive = credit - 1 > 0, 
                    credit = credit - 1, 
                    ...)
      
      else data_object <- readRDS(file)
      
      
    } else if (tolower(tools::file_ext(file)) %in% c("tsv", "csv", "txt")) {
      
      data_object <-
        arrow::open_delim_dataset(sources = file, 
                                  delim = .data_type_delim(tolower(tools::file_ext(file))), 
                                  ...)
      
    } else {
      
      data_object <- arrow::open_dataset(file, ...)
      
    }
    
  }
  
  return(data_object)
  
}


#' Combines a get_data(), map() and bind_rows()
#'
#' @param file file name 
#' @param recursive Should data be recursively loaded?
#' @param credit how many recursive steps are allowed 
#' @param .id name of an optional identifier column (see dplyr::bind_rows())
#'
#' @returns
#' @export
#'
#' @examples
get_data_m <- function(file, recursive = T, credit = 0, .id = NULL) {
  purrr::map(file, \(x) get_data(x, 
                                 recursive = recursive, 
                                 credit = credit)) %>% 
    dplyr::bind_rows(.id = .id)
}


#' List all temporary files saved by write_data() when 
#'
#' @param dir location of temporary files 
#' @param all.paths find all temporary folders 
#' @param pattern pattern/s for files to be removed 
#'
#' @returns
#' @export
#'
#' @examples 
#'   tempdir_list()
tempdir_list <- function(dir = tempdir(), 
                         all.paths = F, 
                         pattern = ".Rds|.parquet|.tsv|.csv|.txt|.pdf") {
  if (all.paths) dir <- dirname(dir)
  list.files(path = dir, pattern = pattern, full.names = T)
}


#' List size of all temporary files saved by write_data() when 
#'
#' @param dir location of temporary files 
#' @param all.paths find all temporary folders 
#' @param pattern pattern/s for files to be removed 
#' @param units unit/s to use to represent file size 
#'
#' @returns
#' @export
#'
#' @examples 
#'   tempdir_list()
tempdir_size <- function(dir = tempdir(), 
                         all.paths = F, 
                         pattern = ".Rds|.parquet|.tsv|.csv|.txt", 
                         units = "auto_si") {
  if (all.paths) dir <- dirname(dir)
  scales::label_bytes(units = "auto_si")(file.size(list.files(path = dir, 
                                                              pattern = pattern, 
                                                              full.names = T)))
}


#' Removing all temporary files saved by write_data() when 
#'
#' @param dir location of temporary files 
#' @param all.paths find all temporary folders 
#' @param pattern pattern/s for files to be removed 
#'
#' @returns
#' @export
#'
#' @examples 
#'   tempdir_remove()
tempdir_remove <- function(dir = tempdir(), 
                           all.paths = F, 
                           pattern = ".Rds|.parquet|.tsv|.csv|.txt|.pdf") {
  if (all.paths) dir <- dirname(dir)
  file.remove(list.files(path = dir, pattern = pattern, full.names = T))
}


#' Saves a nested list as a folder of single files
#'
#' @param object list or single object to be saved
#' @param name name of the folder or file
#' @param dir folder the object is saved in
#' @param type (optional) file type used for every table in the list
#' @param silent Should messages be suppressed?
#' @param redo Should existing files be written again?
#' @param list_as_folders Should nested lists become nested folders?
#' @param clean_memory Should the memory be cleaned with gc()/cleanMem() afer
#' writing
#' @param partitioning Should the parquet files be split into
#' @param ... additional arguments for the saving function
#'
#' @returns TRUE (invisibly)
#' @export
#'
#' @examples
.save_objects_recursively <- function(object, 
                                      name, 
                                      dir, 
                                      type = NULL, 
                                      silent = F, 
                                      redo = T, 
                                      list_as_folders = T, 
                                      clean_memory = F, 
                                      partitioning = NULL, 
                                      ...) {
  
  if (class(object)[1] == "list") {
    if (!silent) cat(paste0('Creating folder "',
                            file.path(dir, name),
                            '".'))
    # re-running a pipeline writes into an existing folder, which is not a problem
    if (!dir.exists(file.path(dir, name)))
      dir.create(file.path(dir, name), recursive = T)
    if (!silent) cat(" Done!\n")
    for (j in names(object)) {
      .save_objects_recursively(object = object[[j]], 
                                name = j, 
                                dir = file.path(dir, name), 
                                type = type, 
                                list_as_folders = list_as_folders, 
                                clean_memory = clean_memory, 
                                silent = silent, 
                                partitioning = partitioning, 
                                ...)
    }
  } else {
    
    # <type> has no default in write_data() and is only passed on if given
    if (is.null(type))
      write_data(x = object, 
                 file = name, 
                 dir = dir, 
                 redo = redo, 
                 list_as_folders = list_as_folders, 
                 clean_memory = clean_memory, 
                 silent = silent, 
                 partitioning = partitioning, 
                 ...)
    
    else
      write_data(x = object, 
                 file = name, 
                 dir = dir, 
                 type = type, 
                 redo = redo, 
                 list_as_folders = list_as_folders, 
                 clean_memory = clean_memory, 
                 silent = silent, 
                 partitioning = partitioning, 
                 ...)
    
  }
  return(invisible(T))
}


#' Reads a folder of files back into a nested list
#'
#' @param name name of the folder or file to read
#' @param dir folder the object lives in
#' @param exclude pattern of entries to return as a path instead of reading
#' @param silent Should messages be suppressed?
#' @param as_arrow_table return tibbles or Arrow connections
#' @param ... additional arguments for the reading function
#'
#' @returns the nested list rebuilt from the folder tree
#' @export
#'
#' @examples
.read_objects_recursively <- function(name,
                                      dir, 
                                      exclude = NULL, 
                                      silent = F, 
                                      as_arrow_table = F, 
                                      ...) {
  
  if (hasArg(dir)) file_dir <- file.path(dir, name)
  else file_dir <- name
  
  if (dir.exists(file_dir)) {
    data_object <- list()
    sub <- list.files(file_dir)
    sub <- sub[list.files(file_dir, full.names = T) %>% 
                 purrr::map_chr(\(x) as.character(file.info(x)$ctime)) %>% 
                 order()]
    for (j in sub) {
      
      data_object[[tools::file_path_sans_ext(j)]] <- 
        .read_objects_recursively(name = j, 
                                  dir = file_dir, 
                                  exclude = if (!is.null(exclude) && 
                                                stringr::str_detect(name, exclude))
                                    paste(exclude, j, sep = "|")
                                  else 
                                    exclude, 
                                  silent = silent)
    }
    
  } else if (!is.null(exclude) && stringr::str_detect(name, exclude)) {
    
    data_object <- file_dir
    
  } else {
    
    data_object <- get_data(file_dir, 
                            recursive = F, 
                            as_arrow_table = as_arrow_table, 
                            ...)
    
  } 
  
  return(data_object)
  
}


#' Loads saved objects from one or several store directories
#'
#' @param dir folder or folders to read
#' @param objects names of the top-level objects to read
#' @param exclude pattern of entries to return as a path instead of reading
#' @param assign assign the objects into the global environment
#' @param silent Should messages be suppressed?
#'
#' @returns the list of read objects (invisibly)
#' @export
#'
#' @examples
load_objects <- function(dir = "",
                         objects = c("Analysis", 
                                     "Datasets", 
                                     "Info"), 
                         exclude = NULL, 
                         assign = T, 
                         silent = F) {
  
  # Loop when multiple directories given 
  if (length(dir) > 1) {
    
    list_of_list_objects <- list() 
    
    for (dir_i in dir) {
      
      list_of_list_objects[[dir_i]] <- load_objects(dir = dir_i, 
                                                    objects = objects, 
                                                    exclude = exclude, 
                                                    assign = assign, 
                                                    silent = silent) 
    }
    
    return(invisible(list_of_list_objects))
    
    
    # Load individual directory 
  } else {
    
    if (!dir.exists(dir)) stop(paste0('<dir> "', 
                                      dir, 
                                      '" not found.'))
    else message(paste0('Reading directory "', dir, '".'))
    
    list_objects <- list()
    
    for (i in objects) {
      if (!file.exists(file.path(dir, i))) 
        message("  ", i, " not found.")
      else {
        
        list_objects[[i]] <- .read_objects_recursively(name = i, 
                                                       dir = dir, 
                                                       exclude = exclude, 
                                                       silent = F)
        
      }
      
    }
    
    if (assign) {
      for (i in names(list_objects)) {
        if (exists(i, where = globalenv(), inherits = F)) {
          message(paste0("  Combining ", i, " with existing data."))
          new_object <- utils::modifyList(get(i, pos = globalenv()), list_objects[[i]])
          assign(i, new_object, pos = globalenv())
        } else {
          message(paste0("  Writing ", i, "."))
          assign(i, list_objects[[i]], pos = globalenv())
        }
      }
    }
    
    return(invisible(list_objects))
    
  }
  
}
