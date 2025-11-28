#' Prints or returns data names
#'
#' @param dataset dataset
#'
#' @return
#' @export
#'
#'
get_data_frame_names <- function(dataset) {
  
  # dataset
  dataset <- get_dataset(dataset)
  
  data_frame_names <- names(Datasets[[dataset]][["Data_frames"]])
  
  data_frame_.names <- names(.Datasets[[dataset]][["Data_frames"]])
  
  
  if (any(intersect(data_frame_names, data_frame_.names) != 
          data_frame_names)) {
    message("Data frame names in Datasets and .Datasets do not fully match. 
            Returning intersection.")
  }
  
  return(intersect(data_frame_names, data_frame_.names))
  
}


#' Assemble data frame from dataset 
#'
#' @param which specific name of data type
#' @param variables selected variables
#' @param observations selected observations
#' @param dataset dataset name 
#' @param output.type data type (default = "tibble", "data.frame", "matrix")
#' @param ... additional arguments for open_data() 
#'
#' @return
#' @export
#'
#'
get_data_frame <- function(which,
                           variables,
                           observations,
                           dataset, 
                           output.type = "tibble", 
                           ...) {
  
  
  # Default data name
  if (!hasArg(which)) {
    stop("Specify the data frame name with <which>.")
  } else if (!is.character(which)) {
    stop("Provide a character string as data frame name with <which>.")
  } else if (length(which) != 1) {
    stop("Provide one data frame name with <which>.")
  }
  
  
  # Checks correct name of dataset
  dataset <- get_dataset(dataset)
  
  
  # Grab data
  data <- .Datasets[[dataset]][["Data_frames"]][[which]] %>% 
    open_data(...)
  
  
  # Assemble variables
  if (hasArg(variables)) {
    variables <- get_variables(variables = {{ variables }},
                               dataset = dataset) %>% 
      dplyr::tibble(variables = .) %>% 
      arrow::arrow_table()
    
    data <- dplyr::inner_join(data, variables, by = "variables") %>% 
      dplyr::compute()
    
  }
  
  # Assemble observations
  if (hasArg(observations)) {
    observations <- get_observations(observations = {{ observations }},
                                     dataset = dataset) %>% 
      dplyr::tibble(observations = .) %>% 
      arrow::arrow_table()
    
    data <- dplyr::inner_join(data, observations, by = "observations") %>% 
      dplyr::compute()
    
  }
  
  
  # Tibble 
  if (grepl(pattern = "tibble", x = output.type)) {
    
    data <- data %>% 
      dplyr::collect()
    
    # Data frame
  } else if (grepl(pattern = "data.frame", x = output.type)) {
    
    data <- data %>% 
      dplyr::collect() %>% 
      tibble2data_frame()
    
    # Matrix
  } else if (grepl(pattern = "matrix", x = output.type)) {
    
    data <- data %>% 
      dplyr::collect() %>%
      tibble2matrix()
    
  } else {
    
    stop("Data type <", output.type, 
         "> is not supported. Use <tibble>, <data.frame> ",
         "or <matrix> instead.", 
         call. = FALSE)
    
  }
  
  # Return
  return(data)
  
}


#' Title
#'
#' @param data_frame 
#' @param data_frame_preview 
#' @param dataset 
#' @param name 
#' @param tag 
#' @param n_preview 
#' @param save_dir 
#' @param partitioning 
#' @param silent 
#'
#' @returns
#' @export
#'
#' @examples
.save_data_frame <- function(data_frame, 
                             data_frame_preview, 
                             dataset, 
                             name = "file", 
                             tag = "", 
                             n_preview = 100, 
                             save_dir, 
                             partitioning, 
                             silent = F) {
  
  # Check dataset 
  dataset <- get_dataset(dataset)
  
  if (!hasArg(save_dir)) 
    save_dir <- tempdir()
  
  else if (!dir.exists(file.path(save_dir, dataset)))
    stop ('Dataset directory does not exist. Use .add_dataset("', dataset, '")')
  
  .Datasets[[dataset]][["Data_frames"]][[name]] <<- 
    write_data(data_frame, 
               file = paste0(dataset, 
                             "/Data_frames/", 
                             stringr::str_remove(paste0(name, tag), 
                                                 "_$")), 
               dir = save_dir, 
               dir.create = ifelse(save_dir == tempdir(), T, F), 
               partitioning = partitioning, 
               silent = silent)
  
  if (!hasArg(data_frame_preview))
    data_frame_preview <- head(data_frame, n_preview)
  
  Datasets[[dataset]][["Data_frames"]][[name]] <<- 
    dplyr::collect(data_frame_preview)
  
  return(invisible(list(preview = Datasets[[dataset]][["Data_frames"]][[name]], 
                        location = .Datasets[[dataset]][["Data_frames"]][[name]])))
  
}


#' Assemble data frame from dataset 
#'
#' @param which specific name of data type
#' @param variables selected variables
#' @param observations selected observations
#' @param dataset dataset name 
#' @param output.type data type (default = "tibble", "data.frame", "matrix")
#' @param ... additional arguments for open_data() 
#'
#' @return
#' @export
#'
#'
open_data_frame <- function(which,
                            variables,
                            observations,
                            dataset, 
                            ...) {
  
  
  # Default data name
  if (!hasArg(which)) {
    stop("Specify the data frame name with <which>.")
  } else if (!is.character(which)) {
    stop("Provide a character string as data frame name with <which>.")
  } else if (length(which) != 1) {
    stop("Provide one data frame name with <which>.")
  }
  
  
  # Checks correct name of dataset
  dataset <- get_dataset(dataset)
  
  
  # Grab data
  data <- .Datasets[[dataset]][["Data_frames"]][[which]] %>% 
    open_data(...)
  
  
  # Assemble variables
  if (hasArg(variables)) {
    variables <- get_variables(variables = {{ variables }},
                               dataset = dataset) %>% 
      dplyr::tibble(variables = .) %>% 
      arrow::arrow_table()
    
    data <- dplyr::inner_join(data, variables, by = "variables") %>% 
      dplyr::compute()
    
  }
  
  # Assemble observations
  if (hasArg(observations)) {
    observations <- get_observations(observations = {{ observations }},
                                     dataset = dataset) %>% 
      dplyr::tibble(observations = .) %>% 
      arrow::arrow_table()
    
    data <- dplyr::inner_join(data, observations, by = "observations") %>% 
      dplyr::compute()
    
  }
  
  # Return
  return(data)
  
}


#' Title
#'
#' @param data 
#' @param dataset 
#' @param name 
#' @param tag 
#' @param n_preview 
#' @param save_dir 
#' @param partitioning 
#'
#' @returns
#' @export
#'
#' @examples
save_data_frame <- function(data, 
                            dataset, 
                            name = "data", 
                            tag = "", 
                            n_preview = 100, 
                            save_dir, 
                            partitioning) {
  
  # Check data input
  if (!hasArg(data)) stop("No data given.")
  
  # Check dataset
  dataset <- get_dataset(dataset)
  
  arrow_object <- any(stringr::str_detect(class(data), "(A|a)rrow"))
  
  
  # # Check observations 
  # if (arrow_object) {
  #   if (any(duplicated(dplyr::pull(data, 
  #                                  "observations", 
  #                                  as_vector = T)))) {
  #     warning("")
  #     
  #     return(data)
  #   }
  # } else {
  #   if (any(duplicated(dplyr::pull(data, 
  #                                  "observations")))) {
  #     warning("")
  #     
  #     return(data)
  #   }
  # }
  # 
  # # Check variables 
  # if (arrow_object) {
  #   if (any(duplicated(dplyr::pull(data, 
  #                                  "variables", 
  #                                  as_vector = T)))) {
  #     warning("")
  #     
  #     return(data)
  #   }
  # } else {
  #   if (any(duplicated(dplyr::pull(data, 
  #                                  "variables")))) {
  #     warning("")
  #     
  #     return(data)
  #   }
  # }
  
  .save_data_frame(data, 
                   dataset = dataset, 
                   name = name, 
                   tag = tag, 
                   n_preview = n_preview, 
                   save_dir = save_dir, 
                   partitioning = partitioning, #= partitioning
                   silent = F)
  
}

