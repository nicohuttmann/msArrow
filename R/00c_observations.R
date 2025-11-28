#' Return observations
#'
#' @param observations vector of observations
#' @param dataset dataset
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
#'
get_observations <- function(observations, dataset, ...) {
  
  # check dataset
  dataset <- get_dataset(dataset)
  
  
  # No observations specified
  # Return all
  if (!hasArg(observations)) {
    observations_output <- .Datasets[[dataset]][["Observations"]] %>%
      get_data(...) %>% 
      dplyr::pull(var = "observations", name = NULL)
    
    return(observations_output)
  }
  
  
  # Check if input is vector
  is_vector_input <- tryCatch(is.atomic(observations),
                              error = function(cond) FALSE)
  
  # if observations input expression
  if (!is_vector_input) {
    observations_output <- .Datasets[[dataset]][["Observations"]] %>%
      get_data(...) %>% 
      dplyr::filter(!!dplyr::enquo(observations)) %>%
      dplyr::pull(var = "observations", name = NULL)
    
    return(observations_output)
    
    # Return all as specified in other function
  } else if (length(observations) == 1 && observations == "all") {
    
    observations_output <- .Datasets[[dataset]][["Observations"]] %>%
      get_data(...) %>% 
      dplyr::pull(var = "observations", name = NULL)
    
    return(observations_output)
    
    # intersect given proteins with proteins in dataset
  } else {
    # All specified observations in observations data
    observations_all <- .Datasets[[dataset]][["Observations"]] %>%
      get_data(...) %>% 
      dplyr::pull(var = "observations", name = NULL)
    
    if (all(observations %in% observations_all)) {
      observations_output <- intersect(observations, observations_all)
      
      return(observations_output)
      
      # Some specified observations match the existing ones
    } else if (any(observations %in% observations_all)) {
      
      stop("Not all specified <observations> were found in the observations ", 
           "table.", 
           call. = FALSE)
      
    } else {
      stop("None of the specified observations were found in the ", 
           "observations table. Try using a tidy-friendly expression.", 
           call. = FALSE)
    }
    
  }
  
}


#' Returns observations data column names
#'
#' @param dataset dataset
#'
#' @return
#' @export
#'
#'
get_observations_data_names <- function(dataset) {
  
  # Get dataset
  dataset <- get_dataset(dataset)
  
  observations_data_names <- names(Datasets[[dataset]][["Observations"]])
  
  observations_.data_names <- names(open_data(.Datasets[[dataset]][["Observations"]]))
  
  if (any(intersect(observations_data_names, observations_.data_names) != 
          observations_data_names)) {
    message("Observations data names in Datasets and .Datasets do not fully match. 
            Returning intersection.")
  }
  
  return(intersect(observations_data_names, observations_.data_names))
  
}


#' Return observations data
#'
#' @param which which observations data columns to pull (multiple supported)
#' @param observations (optional) vector of observations or expression
#' @param dataset dataset
#' @param as_arrow_table return an arrow table 
#' @param ... additional arguments to open_data() 
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
get_observations_data <- function(which,
                                  observations, 
                                  dataset, 
                                  as_arrow_table = F, 
                                  ...) {
  
  # Check dataset
  dataset <- get_dataset(dataset = dataset)
  
  ### Observations
  
  # No observations defined
  if (!hasArg(observations)) {
    
    data <- .Datasets[[dataset]][["Observations"]] %>%
      open_data(...)
    
    # Observations defined
  } else {
    
    # if observations input is vector
    if (!tryCatch(is.atomic(observations),
                  error = function(cond) FALSE)) {
      
      # tidy expression supplied 
      data <- .Datasets[[dataset]][["Observations"]] %>%
        open_data(...) %>% 
        dplyr::filter(!!dplyr::enquo(observations))
      
      # observations supplied as vector of observations 
    } else 
      
      data <- .Datasets[[dataset]][["Observations"]] %>%
        get_data(...) %>% 
        dplyr::filter(.data[["observations"]] %in% !!observations) %>%
        dplyr::arrange(match(.data[["observations"]], !!observations))
    
  } 
  
  
  # Check if names of data were not found in observations_data
  if (hasArg(which) && 
      any(!which %in% get_observations_data_names(dataset = dataset))) 
    stop(paste0("Following observations data column names were not found: ", 
                paste(setdiff(which, 
                              get_observations_data_names(dataset = dataset)), 
                      collapse = ", "), "."))
  
  # Which data to pull
  if (hasArg(which)) {
    data <- data %>% 
      dplyr::select("observations", dplyr::all_of(which))
  }
  
  if (!as_arrow_table) data <- dplyr::collect(data)
  else data <- dplyr::compute(data)
  
  # Return data
  return(data)
  
}


#' Add observations data to data frame
#'
#' @param data data frame
#' @param which observations data
#' @param dataset dataset
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
#'
add_observations_data <- function(data,
                                  which,
                                  dataset) {
  
  # return data if no observations data should be added
  if (!hasArg(which)) return(data)
  
  # Check input
  if (length(names(which)) < length(which)) name <- which
  else name <- names(which)
  
  # Check dataset
  dataset <- get_dataset(dataset)
  
  # Check data type 
  arrow_data <- any(stringr::str_detect(class(data), "(A|a)rrow"))
  
  # Get observations_data
  observations_data <- get_observations_data(which = which, 
                                             dataset = dataset, 
                                             as_arrow_table = arrow_data)
  
  # Check name argument
  if (any(!is.character(name) | name %in% names(data))) {
    stop(paste0("Names for new columns must be strings and cannot exist in the data already."))
  }
  
  
  # Add column/s
  data <- dplyr::left_join(data, observations_data, by = "observations") %>% 
    dplyr::relocate(!!which, .after = c("observations", "variables")) %>% 
    dplyr::compute()
  
  # Return
  return(data)
  
}


#' Title
#'
#' @param observations_data_frame 
#' @param observations_data_frame_preview 
#' @param name 
#' @param tag 
#' @param dataset 
#' @param n_preview 
#' @param save_dir 
#' @param silent 
#'
#' @returns
#' @export
#'
#' @examples
.save_observations_data <- function(observations_data_frame, 
                                    observations_data_frame_preview, 
                                    name = "observations", 
                                    tag = "", 
                                    dataset, 
                                    n_preview = 100, 
                                    save_dir, 
                                    silent = F) {
  
  # Check dataset 
  dataset <- get_dataset(dataset)
  
  if (!hasArg(save_dir)) 
    save_dir <- tempdir()
  
  else if (!dir.exists(file.path(save_dir, dataset)))
    stop ('Dataset directory does not exist. Use .add_dataset("', dataset, '")')
  
  .Datasets[[dataset]][["Observations"]] <<- 
    write_data(observations_data_frame, 
               file = paste0(dataset, 
                             "/Observations/", 
                             stringr::str_remove(paste0(name, tag), 
                                                 "_$")), 
               dir = save_dir, 
               dir.create = ifelse(save_dir == tempdir(), T, F), 
               silent = silent)
  
  if (!hasArg(observations_data_frame_preview))
    observations_data_frame_preview <- head(observations_data_frame, n_preview)
  
  Datasets[[dataset]][["Observations"]] <<- 
    dplyr::collect(observations_data_frame_preview)
  
  return(invisible(list(preview = Datasets[[dataset]][["Observations"]], 
                        location = .Datasets[[dataset]][["Observations"]])))
  
}


#' Title
#'
#' @param data observations data as tibble or Arrow object 
#' @param columns 
#' @param name 
#' @param tag 
#' @param dataset dataset 
#' @param n_preview 
#' @param save_dir 
#'
#' @returns
#' @export
#'
#' @examples
save_observations_data <- function(data, 
                                   columns, 
                                   dataset, 
                                   name = "observations", 
                                   tag = "", 
                                   n_preview = 100, 
                                   save_dir) {
  
  
  # Check data input
  if (!hasArg(data)) stop("No data given.")
  
  # Check dataset
  dataset <- get_dataset(dataset)
  
  arrow_object <- any(stringr::str_detect(class(data), "(A|a)rrow"))
  
  # Check observations 
  if (arrow_object) {
    if (any(duplicated(dplyr::pull(data, 
                                   "observations", 
                                   as_vector = T)))) {
      warning("")
      
      return(data)
    }
  } else {
    if (any(duplicated(dplyr::pull(data, 
                                   "observations")))) {
      warning("")
      
      return(data)
    }
  }
  
  
  
  # Get template
  template <- tryCatch(get_observations_data(dataset = dataset, 
                                          as_arrow_table = arrow_object), 
                       error = function(e) NULL)
  
  if (!is.null(template)) {
  
  # If no columns specified, test for overlapping names 
  if (!hasArg(columns)) {
    
    if (any("observations" != intersect(names(data),
                                        get_observations_data_names(dataset)))) {
      warning("Columns cannot be overwritten if <column> argument is not specified.")
      
      return(invisible(data))
    }
    
    columns <- setdiff(names(data), "observations")
    
    
  } else {
    
    # Get template
    template <- template %>% 
      dplyr::select(-dplyr::any_of(columns))
    
  }
  
  # Keep only columns to save 
  data_to_add <- data %>% 
    dplyr::select(dplyr::all_of(c("observations", 
                                  columns)))
  
  
  observations_data_new <- dplyr::left_join(template, data_to_add, 
                                            by = "observations") %>% 
    dplyr::compute()
  
  } else {
    
    if (!hasArg(columns)) observations_data_new <- data
    
    else observations_data_new <- data %>% 
        dplyr::select(dplyr::all_of(c("observations", 
                                      columns)))
  }
  
  
  .save_observations_data(observations_data_frame = observations_data_new, 
                          name = "observations", 
                          tag = tag, 
                          dataset = dataset, 
                          n_preview = n_preview, 
                          save_dir = save_dir)
  
  return(invisible(data))
  
}
