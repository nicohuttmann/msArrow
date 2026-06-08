#' Checks and returns correct dataset identifier
#'
#' @param dataset dataset name 
#'
#' @return
#' @export
#'
#'
get_dataset <- function(dataset) {
  
  # Check if Datasets list exist
  if (!exists("Datasets")) 
    stop("There must be a list named Datasets.")
  
  if (!exists(".Datasets")) 
    stop("There must be a list named .Datasets.")
  
  
  # Automatic return if only one dataset exists
  if (!hasArg(dataset) && length(Datasets) == 1) {
    
    dataset <- names(Datasets)
    
  } else if (!hasArg(dataset)) {
    stop("Please specify a dataset.")
  }
  
  
  # Name correct
  if (dataset %in% names(Datasets)) {
    if (dataset %in% names(.Datasets)) {
      return(dataset)
    } else {
      stop("Dataset present in Datasets but not in .Datasets.")
    }
  } else {
    stop("Dataset could not be found in object Datasets.")
  }
  
}


#' Prints or returns all dataset names
#'
#' @return
#' @export
#'
#'
get_dataset_names <- function() {
  
  # Check if Datasets list exist
  if (!exists("Datasets")) 
    stop("There must be a list named Datasets.")
  
  if (!exists(".Datasets")) 
    stop("There must be a list named .Datasets.")
  
  names_datasets <- names(Datasets)
  
  names_.datasets <- names(.Datasets)
  
  if (any(intersect(names_datasets, names_.datasets) != names_datasets)) {
    message("Names in Datasets and .Datasets do not fully match. Returning intersection.")
  }
  
  # Return
  return(intersect(names_datasets, names_.datasets))
  
}

#' Title
#'
#' @param name 
#' @param save_dir 
#' @param replace 
#'
#' @returns
#' @export
#'
#' @examples
.add_dataset <- function(name, save_dir, replace = F) {
  
  if (!exists(".Datasets", where = globalenv())) 
    assign(".Datasets", list(), pos = globalenv())
  
  if (!exists("Datasets", where = globalenv())) 
    assign("Datasets", list(), pos = globalenv())
  
  if (!replace) {
    if (name %in% get_dataset_names()) 
      warning("Dataset name already exists in .Datasets.")
  }
  
  if (!hasArg(save_dir)) {
    if (!is.null(.get_defaults("save_dir")))
      save_dir <- .get_defaults("save_dir")
    else 
      save_dir <- tempdir()
  }
  
  purrr::walk(file.path(save_dir, name, 
                        c("", 
                          "Variables", 
                          "Observations", 
                          "Data_frames")), 
              \(x) if (!dir.exists(x)) dir.create(x, recursive = T))
  
  .Datasets[[name]] <<- list(Variables = c(), 
                             Observations = c(), 
                             Data_frames = list())
  
  Datasets[[name]] <<- list(Variables = c(), 
                            Observations = c(), 
                            Data_frames = list())
  
  return(invisible(TRUE))
  
}


#' Change name or copy data before saving the RData environment
#'
#' @param tag suffix for data names 
#' @param copy.files should data frames with other names be copied (default = F)
#'
#' @returns
#' @export
#'
#' @examples
retag_datasets <- function(tag = "_x", copy.files = FALSE) {
  
  if (tag == "_x") {
    message("Please change the tag name.")
    return(invisible(FALSE))
  }
  
  for (.dataset in names(.Datasets)) {
    
    # Variables 
    file <- .Datasets[[.dataset]][["Variables"]] 
    
    if (is.character(file)) {
      
      file.name <- basename(file)
      
      dir.name <- dirname(file)
      
      name <- tools::file_path_sans_ext(file.name)
      
      ext <- stringr::str_extract(file.name, "\\..+?$")
      
      new.file <- file.path(dir.name, paste0("variables", tag, ext))
      
      if (name == "variables") {
        file.rename(file, new.file)
      } else {
        file.copy(file, new.file)
      }
      
      .Datasets[[.dataset]][["Variables"]] <<- new.file
      
    }
    
    
    # Observations 
    file <- .Datasets[[.dataset]][["Observations"]] 
    
    if (is.character(file)) {
      
      file.name <- basename(file)
      
      dir.name <- dirname(file)
      
      name <- tools::file_path_sans_ext(file.name)
      
      ext <- stringr::str_extract(file.name, "\\..+?$")
      
      new.file <- file.path(dir.name, paste0("observations", tag, ext))
      
      if (name == "observations") {
        file.rename(file, new.file)
      } else {
        file.copy(file, new.file)
      }
      
      .Datasets[[.dataset]][["Observations"]] <<- new.file
      
    }
    
    # Data frames 
    for (.data_frame in names(.Datasets[[.dataset]][["Data_frames"]])) {
      
      file <- .Datasets[[.dataset]][["Data_frames"]][[.data_frame]]
      
      if (is.character(file)) {
        
        file.name <- basename(file)
        
        dir.name <- dirname(file)
        
        name <- tools::file_path_sans_ext(file.name)
        
        ext <- stringr::str_remove(file.name, name)
        
        new.file <- file.path(dir.name, paste0(.data_frame, tag, ext))
        
        if (name == .data_frame) {
          file.rename(file, new.file)
        .Datasets[[.dataset]][["Data_frames"]][[.data_frame]] <<- new.file
        } else if (copy.files) {
          file.copy(file, new.file)
        }
        
        
      }
      
    }
    
  }
  
  message("Done.")
  
  return(invisible(TRUE))
  
}

