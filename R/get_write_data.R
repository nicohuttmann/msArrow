#' Title
#'
#' @param x 
#' @param file 
#' @param dir 
#' @param dir.create 
#' @param clean_memory 
#' @param silent 
#' @param partitioning 
#'
#' @returns
#' @export
#'
#' @examples
write_data <- function(x, file, dir, dir.create = F, clean_memory = F, silent = T, partitioning) {
  
  if (!hasArg(x)) stop("No data <x> given.")
  
  if (!hasArg(dir) & !hasArg(file)) {
    file <- tempfile()
    #stop("Please provide the data <x> and a <file>.")
  } else if (!hasArg(file)) {
    stop("Please provide the data <x> and a <file> name when specifying a directory <dir>.")
  }
  
  
  if (hasArg(dir)) file_dir <- file.path(dir, file)
    else file_dir <- file
    
   if (dir.create) dir.create(dirname(file_dir), recursive = T, showWarnings = F)
    
    
  if ((tibble::is_tibble(x) | 
      is.data.frame(x) | 
      is.matrix(x) | 
      any(c("ArrowObject", "arrow_dplyr_query") %in% class(x))) & 
      length(tryCatch(arrow::infer_type(x), error = function(e) NULL)) > 0) {
    
    # if (!stringr::str_detect(tolower(file_dir), "parquet") && 
    #     !hasArg(partitioning)) file_dir <- paste0(file_dir, ".parquet")
    
    if (!stringr::str_detect(tolower(file_dir), "parquet")) 
      file_dir <- paste0(file_dir, ".parquet")
    
    
    
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
    
    if (!hasArg(partitioning) || 
        !partitioning %in% names(x)) arrow::write_parquet(x, file_dir)
    
    else arrow::write_dataset(x, file_dir, partitioning = partitioning)
    
  } else {
    
    if (!stringr::str_detect(tolower(file_dir), "rds")) 
      file_dir <- paste0(file_dir, ".Rds")
    
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
    
    saveRDS(x, file_dir)
    
  }
  
  if (!silent) cat(" Done!\n")
  
  if (!isFALSE(clean_memory)) cleanMem(clean_memory)
  
  return(file_dir)
  
}


#' Loads data from a file name 
#'
#' @param file file name 
#' @param recursive Should data be recursively loaded?
#' @param credit how many recursive steps are allowed 
#' @param as_arrow_table return a tibble or an Arrow connection 
#'
#' @returns
#' @export
#'
#' @examples
get_data <- function(file, 
                     recursive = T, 
                     credit = 10, 
                     as_arrow_table = F, 
                     ...) {
  
  if (length(file) > 1) {
    
    return(file)
    
  } else {
    
    if (!is.character(file)) {
      
      if (class(file)[1] == "list") data_object <- purrr::map(file, get_data)
      else data_object <- file
      
    } else if (tolower(tools::file_ext(file)) == "parquet") {
      
      if (!as_arrow_table) {
        
        data_object <- tibble::as_tibble(nanoparquet::read_parquet(file))
        
      } else {
        
        # data_object <- arrow::read_parquet(file, as_data_frame = F)
        data_object <- arrow::open_dataset(file, ...)
        
      }
      
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
#' @param recursive Should data be recursively loaded?
#' @param credit how many recursive steps are allowed 
#'
#' @returns
#' @export
#'
#' @examples
open_data <- function(file, 
                     recursive = T, 
                     credit = 10, 
                     ...) {
  
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
#' @param pattern pattern/s for files to be removed 
#'
#' @returns
#' @export
#'
#' @examples 
#'   tempdir_list()
tempdir_list <- function(dir = tempdir(), pattern = ".Rds|.parquet") {
  list.files(path = dir, pattern = pattern, full.names = T)
}


#' List size of all temporary files saved by write_data() when 
#'
#' @param dir location of temporary files 
#' @param pattern pattern/s for files to be removed 
#' @param units unit/s to use to represent file size 
#'
#' @returns
#' @export
#'
#' @examples 
#'   tempdir_list()
tempdir_size <- function(dir = tempdir(), 
                         pattern = ".Rds|.parquet", 
                         units = "auto_si") {
  scales::label_bytes(units = "auto_si")(file.size(list.files(path = dir, 
                                                              pattern = pattern, 
                                                              full.names = T)))
}


#' Removing all temporary files saved by write_data() when 
#'
#' @param dir location of temporary files 
#' @param pattern pattern/s for files to be removed 
#'
#' @returns
#' @export
#'
#' @examples 
#'   tempdir_remove()
tempdir_remove <- function(dir = tempdir(), pattern = ".Rds|.parquet") {
  file.remove(list.files(path = dir, pattern = pattern, full.names = T))
}





#' Title
#'
#' @param object 
#' @param name 
#' @param dir 
#' @param silent 
#'
#' @returns
#' @export
#'
#' @examples
.save_objects_recursively <- function(object, name, dir, silent = F) {
  
  if (class(object)[1] == "list") {
    if (!silent) cat(paste0('Creating folder "', 
                            file.path(dir, name), 
                            '".'))
    dir.create(file.path(dir, name))
    if (!silent) cat(" Done!\n")
    for (j in names(object)) {
      .save_objects_recursively(object = object[[j]], 
                                name = j, 
                                dir = file.path(dir, name))
    }
  } else if (class(object)[1] %in% c("tbl_df", "tbl")) {
    if (!silent) cat(paste0('Saving file "', 
                            file.path(dir, paste0(name, ".parquet")), 
                            '".'))
    arrow::write_parquet(object, 
                         file.path(dir, paste0(name, ".parquet")))
    if (!silent) cat(" Done!\n")
  } else {
    if (!silent) cat(paste0('Saving file "', 
                            file.path(dir, paste0(name, ".Rds")), 
                            '".'))
    saveRDS(object, 
            file.path(dir, paste0(name, ".Rds")))
    if (!silent) cat(" Done!\n")
  }
  return(invisible(T))
}


#' Title
#'
#' @param objects 
#' @param dir 
#' @param silent 
#'
#' @returns
#' @export
#'
#' @examples
save_objects <- function(objects = c("Analysis", 
                                     "Datasets", 
                                     "Info"), 
                         dir = "Data/RData/temp", 
                         silent = F) {
  
  message(paste0('Starting to save following files under "', 
                 dir, 
                 '":\n', 
                 paste(paste0("-", objects), collapse = "\n")))
  
  if (!dir.exists(dir)) dir.create(dir, recursive = T)
  
  for (i in objects) {
    if (!exists(i)) warning(i, "  not found.")
    else .save_objects_recursively(object = eval(parse(text = i)), 
                                   name = i, 
                                   dir = dir, 
                                   silent = F)
  }
  
  message("All done!")
  
}


#' Title
#'
#' @param name 
#' @param dir 
#' @param exclude 
#' @param silent 
#'
#' @returns
#' @export
#'
#' @examples
.read_objects_recursively <- function(name, dir, exclude = NULL, silent = F) {
  
  if (dir.exists(file.path(dir, name))) {
    data_object <- list()
    sub <- list.files(file.path(dir, name))
    sub <- sub[list.files(file.path(dir, name), full.names = T) %>% 
                 purrr::map_chr(\(x) as.character(file.info(x)$ctime)) %>% 
                 order()]
    for (j in sub) {
      
      data_object[[tools::file_path_sans_ext(j)]] <- 
        .read_objects_recursively(name = j, 
                                  dir = file.path(dir, name), 
                                  exclude = if (!is.null(exclude) && 
                                                stringr::str_detect(name, exclude))
                                    paste(exclude, j, sep = "|")
                                  else 
                                    exclude, 
                                  silent = silent)
    }
    
  } else if (!is.null(exclude) && stringr::str_detect(name, exclude)) {
    
    data_object <- file.path(dir, name)
    
  } else {
    
    data_object <- get_data(file.path(dir, name), recursive = F)
    
  } 
  
  return(data_object)
  
}


#' Title
#'
#' @param dir 
#' @param objects 
#' @param exclude 
#' @param silent 
#'
#' @returns
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
