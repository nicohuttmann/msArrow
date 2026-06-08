#' Writer function for get_variables_data
#'
#' @param copy2clipboard copy the code to the clipboard
#' @param graphics use a graphical interface for selection 
#' @param change_output.type change output type to other than "tibble"
#'
#' @return
#' @export
#'
#' 
.cat_get_variables_data <- function(copy2clipboard = T, 
                                    graphics = F, 
                                    change_output.type = F) {
  
  dataset <- select.list(get_dataset_names(), 
                         title = "Choose a dataset:", 
                         multiple = F, graphics = graphics)
  
  which <- select.list(get_variables_data_names(dataset), 
                       title = "Choose variables data columns:", 
                       multiple = T, 
                       graphics = graphics)
  
  
  if (!change_output.type) {
    
    output <- paste(
      paste0('get_variables_data(which = ', 
             paste0('c("',
                    paste(which, collapse = '",\n\t"'),
                    '")')), 
      'variables = ', 
      paste0('dataset = "', dataset, '")'), 
      sep = ',\n\t')
    
    if (copy2clipboard)
      cat(output, 
          file = "clipboard")
    
    cat(output)
    
    return(invisible(output))
    
  }
  
  # Add output type
  output.type <- select.list(c("tibble", 
                               "data.frame", 
                               "vector", 
                               "list"), 
                             preselect = "tibble", 
                             title = "Choose the output type:", 
                             multiple = F, 
                             graphics = graphics)
  
  output <- paste(
    paste0('get_variables_data(which = ', 
           paste0('c("',
                  paste(which, collapse = '",\n\t"'),
                  '")')), 
    'variables = ', 
    paste0('dataset = "', dataset, '"'), 
    paste0('output.type = "', output.type, '")'), 
    sep = ',\n\t')
  
  if (copy2clipboard)
    cat(output, 
        file = "clipboard")
  
  cat(output)
  
  return(invisible(output))
  
}


#' Writer function for get_observations_data
#'
#' @param copy2clipboard copy the code to the clipboard
#' @param graphics use a graphical interface for selection 
#' @param change_output.type change output type to other than "tibble"
#'
#' @return
#' @export
#'
#' 
.cat_get_observations_data <- function(copy2clipboard = T, 
                                       graphics = F, 
                                       change_output.type = F) {
  
  dataset <- select.list(get_dataset_names(), 
                         title = "Choose a dataset:", 
                         multiple = F, graphics = graphics)
  
  which <- select.list(get_observations_data_names(dataset), 
                       title = "Choose observations data columns:", 
                       multiple = T, 
                       graphics = graphics)
  
  
  if (!change_output.type) {
    
    output <- paste(
      paste0('get_observations_data(which = ', 
             paste0('c("',
                    paste(which, collapse = '",\n\t"'),
                    '")')), 
      'observations = ', 
      paste0('dataset = "', dataset, '")'), 
      sep = ',\n\t')
    
    if (copy2clipboard)
      cat(output, 
          file = "clipboard")
    
    cat(output)
    
    return(invisible(output))
    
  }
  
  # Add output type
  output.type <- select.list(c("tibble", 
                               "data.frame", 
                               "vector", 
                               "list"), 
                             preselect = "tibble", 
                             title = "Choose the output type:", 
                             multiple = F, 
                             graphics = graphics)
  
  output <- paste(
    paste0('get_observations_data(which = ', 
           paste0('c("',
                  paste(which, collapse = '",\n\t"'),
                  '")')), 
    'observations = ', 
    paste0('dataset = "', dataset, '"'), 
    paste0('output.type = "', output.type, '")'), 
    sep = ',\n\t')
  
  if (copy2clipboard)
    cat(output, 
        file = "clipboard")
  
  cat(output)
  
  return(invisible(output))
  
}


#' Writer function for get_data_frame
#'
#' @param copy2clipboard copy the code to the clipboard
#' @param graphics use a graphical interface for selection 
#' @param change_output.type change output type to other than "tibble"
#'
#' @return
#' @export
#'
#' 
.cat_get_data_frame <- function(copy2clipboard = T, 
                                graphics = F, 
                                change_output.type = F) {
  
  dataset <- select.list(get_dataset_names(), 
                         title = "Choose a dataset:", 
                         multiple = F, graphics = graphics)
  
  which <- select.list(get_data_frame_names(dataset), 
                       title = "Choose data frame:", 
                       multiple = F, 
                       graphics = graphics)
  
  
  if (!change_output.type) {
    
    output <- paste(
      paste0('get_data_frame(which = ', 
             paste0('"', which, '"')), 
      'variables = ', 
      'observations = ', 
      paste0('dataset = "', dataset, '")'), 
      sep = ',\n\t')
    
    if (copy2clipboard)
      cat(output, 
          file = "clipboard")
    
    cat(output)
    
    return(invisible(output))
    
  }
  
  # Add output type
  output.type <- select.list(c("tibble", 
                               "data.frame", 
                               "matrix"), 
                             preselect = "tibble", 
                             title = "Choose the output type:", 
                             multiple = F, 
                             graphics = graphics)
  
  output <- paste(
    paste0('get_data_frame(which = ', 
           paste0('"', which, '"')), 
    'variables = ', 
    'observations = ', 
    paste0('dataset = "', dataset, '"'), 
    paste0('output.type = "', output.type, '")'), 
    sep = ',\n\t')
  
  if (copy2clipboard)
    cat(output, 
        file = "clipboard")
  
  cat(output)
  
  return(invisible(output))
  
}


#' Writer function for get_data_frame
#'
#' @param copy2clipboard copy the code to the clipboard
#' @param graphics use a graphical interface for selection 
#' @param change_output.type change output type to other than "tibble"
#'
#' @return
#' @export
#'
#' 
.cat_get_data_frame_m <- function(copy2clipboard = T, 
                                  graphics = F, 
                                  change_output.type = F) {
  
  dataset <- select.list(get_dataset_names(), 
                         title = "Choose a dataset:", 
                         multiple = F, graphics = graphics)
  
  which <- select.list(get_data_frame_names(dataset), 
                       title = "Choose data frames:", 
                       multiple = T, 
                       graphics = graphics)
  
  
  if (!change_output.type) {
    
    output <- paste(
      paste0('get_data_frame_m(which = ', 
             paste0('c("',
                    paste(which, collapse = '",\n\t"'),
                    '")')), 
      'variables = ', 
      'observations = ', 
      paste0('dataset = "', dataset, '")'), 
      sep = ',\n\t')
    
    if (copy2clipboard)
      cat(output, 
          file = "clipboard")
    
    cat(output)
    
    return(invisible(output))
    
  }
  
  # Add output type
  output.type <- select.list(c("tibble", 
                               "data.frame", 
                               "matrix"), 
                             preselect = "tibble", 
                             title = "Choose the output type:", 
                             multiple = F, 
                             graphics = graphics)
  
  output <- paste(
    paste0('get_data_frame_m(which = ', 
           paste0('c("',
                  paste(which, collapse = '",\n\t"'),
                  '")')), 
    'variables = ', 
    'observations = ', 
    paste0('dataset = "', dataset, '"'), 
    paste0('output.type = "', output.type, '")'), 
    sep = ',\n\t')
  
  if (copy2clipboard)
    cat(output, 
        file = "clipboard")
  
  cat(output)
  
  return(invisible(output))
  
}

