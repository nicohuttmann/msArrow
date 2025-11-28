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
  
  if (!hasArg(save_dir)) 
    save_dir <- tempdir()
  
  purrr::walk(file.path(save_dir, name, 
                        c("", 
                          "Variables", 
                          "Observations", 
                          "Data_frames")), 
              \(x) dir.create(x, recursive = T))
  
  .Datasets[[name]] <<- list(Variables = c(), 
                             Observations = c(), 
                             Data_frames = list())
  
  Datasets[[name]] <<- list(Variables = c(), 
                            Observations = c(), 
                            Data_frames = list())
  
  return(invisible(TRUE))
  
}
