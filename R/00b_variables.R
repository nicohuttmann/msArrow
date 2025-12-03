#' Return variables
#'
#' @param variables vector of variables
#' @param dataset dataset
#' @param ... additional arguments to open_data() 
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
#'
get_variables <- function(variables, dataset, ...) {
  
  # Check dataset
  dataset <- get_dataset(dataset)
  
  
  # No variables specified
  if (!hasArg(variables)) {
    variables_output <- .Datasets[[dataset]][["Variables"]] %>%
      open_data(...) %>% 
      dplyr::pull(var = "variables", name = NULL, as_vector = T)
    return(variables_output)
  }
  
  
  # Check if input is vector
  is_vector_input <- tryCatch(is.atomic(variables),
                              error = function(cond) FALSE)
  
  
  # if variables input expression
  if (!is_vector_input) {
    variables_output <- .Datasets[[dataset]][["Variables"]] %>%
      open_data(...) %>% 
      dplyr::filter(!!dplyr::enquo(variables)) %>%
      dplyr::pull(var = "variables", name = NULL, as_vector = T)
    return(variables_output)
    
    # input given as vector
    # intersect given proteins with proteins in dataset
  } else {
    variables_output <- intersect(variables,
                                  .Datasets[[dataset]][["Variables"]] %>%
                                    open_data(...) %>% 
                                    dplyr::pull(var = "variables", name = NULL, as_vector = T))
    return(variables_output)
    
  }
  
}


#' Returns variables data column names
#'
#' @param dataset dataset
#'
#' @return
#' @export
#'
#'
get_variables_data_names <- function(dataset) {
  
  # Get dataset name
  dataset <- get_dataset(dataset)
  
  variables_data_names <- names(Datasets[[dataset]][["Variables"]])
  
  variables_.data_names <- names(open_data(.Datasets[[dataset]][["Variables"]]))
  
  if (any(intersect(variables_data_names, variables_.data_names) != 
          variables_data_names)) {
    message("Variables data names in Datasets and .Datasets do not fully match. 
            Returning intersection.")
  }
  
  return(intersect(variables_data_names, variables_.data_names))
  
}


#' Return variables data
#'
#' @param which which variables data columns to pull (multiple supported)
#' @param variables (optional) vector of variables or expression
#' @param dataset dataset
#' @param as_arrow_table return an arrow table 
#' @param ... additional arguments to open_data() 
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
get_variables_data <- function(which,
                               variables, 
                               dataset, 
                               as_arrow_table = F, 
                               ...) {
  
  # Check dataset
  dataset <- get_dataset(dataset = dataset)
  
  ### Variables
  
  # No variables defined
  if (!hasArg(variables)) {
    
    data <- .Datasets[[dataset]][["Variables"]] %>%
      open_data(...)
    
    # Variables defined
  } else {
    
    # if variables input is vector
    if (!tryCatch(is.atomic(variables),
                  error = function(cond) FALSE)) {
      
      # tidy expression supplied 
      data <- .Datasets[[dataset]][["Variables"]] %>%
        open_data(...) %>% 
        dplyr::filter(!!dplyr::enquo(variables))
      
      # variables supplied as vector of variables 
    } else 
      
      data <- .Datasets[[dataset]][["Variables"]] %>%
        get_data(...) %>% 
        dplyr::filter(.data[["variables"]] %in% !!variables) %>%
        dplyr::arrange(match(.data[["variables"]], !!variables))
    
  } 
  
  
  # Check if names of data were not found in variables_data
  if (hasArg(which) && 
      any(!which %in% get_variables_data_names(dataset = dataset))) 
    stop(paste0("Following variables data column names were not found: ", 
                paste(setdiff(which, 
                              get_variables_data_names(dataset = dataset)), 
                      collapse = ", "), "."))
  
  # Which data to pull
  if (hasArg(which)) {
    data <- data %>% 
      dplyr::select("variables", dplyr::all_of(which))
  }
  
  if (!as_arrow_table) data <- dplyr::collect(data)
  else data <- dplyr::compute(data)
  
  # Return data
  return(data)
  
}


#' Add variables data to data frame
#'
#' @param data data frame
#' @param which variables data
#' @param dataset dataset
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
#'
add_variables_data <- function(data,
                               which,
                               dataset) {
  
  # return data if no variables data should be added
  if (!hasArg(which)) return(data)
  
  # Check input
  if (length(names(which)) < length(which)) name <- which
  else name <- names(which)
  
  # Check dataset
  dataset <- get_dataset(dataset)
  
  # Check data type 
  arrow_object <- any(stringr::str_detect(class(data), "(A|a)rrow"))
  
  # Get variables_data
  variables_data <- get_variables_data(which = which, 
                                       dataset = dataset, 
                                       as_arrow_table = arrow_object)
  
  # Check name argument
  if (any(!is.character(name) | name %in% names(data))) {
    stop(paste0("Names for new columns must be strings and cannot exist in the data already."))
  }
  
  
  # Add column/s
  if ("observations" %in% names(data)) {
  data <- dplyr::left_join(data, variables_data, by = "variables") %>% 
    dplyr::relocate(!!which, .after = c("observations", "variables")) %>% 
    dplyr::compute()
  } else {
    data <- dplyr::left_join(data, variables_data, by = "variables") %>% 
      dplyr::relocate(!!which, .after = c("observations", "variables")) %>% 
      dplyr::compute()
  }
  
  # Return
  return(data)
  
}


#' Title
#'
#' @param variables_data_frame 
#' @param variables_data_frame_preview 
#' @param dataset 
#' @param name 
#' @param tag 
#' @param n_preview 
#' @param save_dir 
#'
#' @returns
#' @export
#'
#' @examples
.save_variables_data <- function(variables_data_frame, 
                                 variables_data_frame_preview, 
                                 dataset, 
                                 name = "variables", 
                                 tag = "", 
                                 n_preview = 100, 
                                 save_dir) {
  
  # Check dataset 
  dataset <- get_dataset(dataset)
  
  if (!hasArg(save_dir)) 
    save_dir <- tempdir()
  
  else if (!dir.exists(file.path(save_dir, dataset)))
    stop ('Dataset directory does not exist. Use .add_dataset("', dataset, '")')
  
  .Datasets[[dataset]][["Variables"]] <<- 
    write_data(variables_data_frame, 
               file = paste0(dataset, 
                             "/Variables/", 
                             stringr::str_remove(paste0(name, tag), 
                                                 "_$")), 
               dir = save_dir, 
               dir.create = ifelse(save_dir == tempdir(), T, F), 
               silent = F)
  
  if (!hasArg(variables_data_frame_preview))
    variables_data_frame_preview <- head(variables_data_frame, n_preview)
  
  Datasets[[dataset]][["Variables"]] <<- 
    dplyr::collect(variables_data_frame_preview)
  
  
  return(invisible(list(preview = Datasets[[dataset]][["Variables"]], 
                        location = .Datasets[[dataset]][["Variables"]])))
  
}


#' Title
#'
#' @param data variables data as tibble or Arrow object 
#' @param columns 
#' @param name 
#' @param tag 
#' @param dataset 
#' @param n_preview 
#' @param save_dir 
#'
#' @returns
#' @export
#'
#' @examples
save_variables_data <- function(data, 
                                columns, 
                                name = "variables", 
                                tag = "", 
                                dataset, 
                                n_preview = 100, 
                                save_dir) {
  
  
  # Check data input
  if (!hasArg(data)) stop("No data given.")
  
  # Check dataset
  dataset <- get_dataset(dataset)
  
  arrow_object <- any(stringr::str_detect(class(data), "(A|a)rrow"))
  
  # Check variables 
  if (arrow_object) {
    if (any(duplicated(dplyr::pull(data, 
                                   "variables", 
                                   as_vector = T)))) {
      warning("")
      
      return(data)
    }
  } else {
    if (any(duplicated(dplyr::pull(data, 
                                   "variables")))) {
      warning("")
      
      return(data)
    }
  }
  
  
  
  # Get template
  template <- tryCatch(get_variables_data(dataset = dataset, 
                                          as_arrow_table = arrow_object), 
                       error = function(e) NULL)
  
  if (!is.null(template)) {
    
    # If no columns specified, test for overlapping names 
    if (!hasArg(columns)) {
      
      if (any("variables" != intersect(names(data),
                                       get_variables_data_names(dataset)))) {
        warning("Columns cannot be overwritten if <column> argument is not specified.")
        
        return(invisible(data))
      }
      
      columns <- setdiff(names(data), "variables")
      
      
    } else {
      
      # Get template
      template <- template %>% 
        dplyr::select(-dplyr::any_of(columns))
      
    }
    
    # Keep only columns to save 
    data_to_add <- data %>% 
      dplyr::select(dplyr::all_of(c("variables", 
                                    columns)))
    
    
    variables_data_new <- dplyr::left_join(template, data_to_add, 
                                           by = "variables") %>% 
      dplyr::compute()
    
  } else {
    
    if (!hasArg(columns)) variables_data_new <- data
    
    else variables_data_new <- data %>% 
        dplyr::select(dplyr::all_of(c("variables", 
                                      columns)))
  }
  
  
  .save_variables_data(variables_data_frame = variables_data_new, 
                       name = "variables", 
                       tag = tag, 
                       dataset = dataset, 
                       n_preview = n_preview, 
                       save_dir = save_dir)
  
  return(invisible(data))
  
}
