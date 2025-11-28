#' Same as strsplit but option to return as vector and to convert to numeric
#'
#' @param x string
#' @param split split character
#' @param vector.output output type (T = vector; F = list)
#' @param as.numeric try to convert to numeric
#'
#' @return
#' @export
#'
#'
strsplit_ <- function(x, split, vector.output = T, as_numeric = F) {
  
  # Check if input is given
  if (!hasArg(x)) stop("No argument specified for <x>.", call. = FALSE)
  
  # Identifies separator if not given
  if (!hasArg(split)) stop("No argument specified for <split>.", call. = FALSE)
  
  # Check if input is list
  if (is.list(x) || is.atomic(x)) {
    
    x <- unlist(x)
    
    # Check if list contains only character
    if (all(unlist(lapply(x, is.character)))) {
      x_split <- strsplit(x = x, split = split)
    } else if (any(unlist(lapply(x, is.character)))) {
      x_split <- strsplit(x = lapply(x, as.character), split = split)
    } else {
      x_split <- as.list(x)
    }
    
    # No list or vector input
  } else {
    stop("Unknown input type for this function. <x> must be a list or a ", 
         "vector." , call. = FALSE)
  }
  
  
  # Coerce to numeric if possible
  if (as_numeric &&
      !any(unlist(suppressWarnings(
        lapply(x, function(x) any(is.na(as.numeric(x[!is.na(x)])))))))) {
    x_split <- lapply(x, as.numeric)
  }
  
  # Return list or vector
  if (!vector.output) {
    return(x_split)
  } else {
    return(unlist(x_split))
  }
  
}


#' Performs strsplit and keeps elements specified by vector
#'
#' @param x vector
#' @param split separator
#' @param keep vector of elements to keep
#' @param as_numeric try to convert to numeric
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
strsplit_keep <- function(x, split, keep = 1, as_numeric = F) {
  
  # Separates strings and keeps first element of each vector
  x <- x %>%
    strsplit_(split = split, vector.output = F, as_numeric = as_numeric) %>%
    lapply(FUN = function(x) na.omit(x[keep])) %>%
    lapply(FUN = function(x) paste(x, collapse = split)) %>%
    unlist() %>%
    unname()
  
  return(x)
  
}


#' Performs strsplit and keeps first n elements of each string
#'
#' @param x vector
#' @param split separator
#' @param n number of elements to keep counting from the beginning
#' @param as_numeric try to convert to numeric
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
strsplit_keep_firstn <- function(x, split, n = 1, as_numeric = F) {
  
  # Separates strings and keeps first element of each vector
  x <- x %>%
    strsplit_(split = split, vector.output = F, as_numeric = as_numeric) %>%
    lapply(FUN = function(x) x[1:(min(c(length(x), n)))]) %>%
    lapply(FUN = function(x) paste(x, collapse = split)) %>%
    unlist() %>%
    unname()
  
  return(x)
  
}


#' Performs strsplit and keeps first element of each string
#'
#' @param x vector
#' @param split separator
#' @param as_numeric try to convert to numeric
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
strsplit_keep_first <- function(x, split, as_numeric = F) {
  
  strsplit_keep_firstn(x = x, n = 1, split = split, as_numeric = as_numeric)
  
}



#' Performs strsplit and keeps first n elements of each string
#'
#' @param x vector
#' @param split separator
#' @param n number of elements to keep counting from the end
#' @param as_numeric try to convert to numeric
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
strsplit_keep_lastn <- function(x, split, n = 1, as_numeric = F) {
  
  # Separates strings and keeps first element of each vector
  x <- x %>%
    strsplit_(split = split, vector.output = F, as_numeric = as_numeric) %>%
    lapply(rev) %>% 
    lapply(FUN = function(x) x[1:(min(c(length(x), n)))]) %>%
    lapply(rev) %>% 
    lapply(FUN = function(x) paste(x, collapse = split)) %>%
    unlist() %>%
    unname()
  
  return(x)
  
}


#' Performs strsplit and keeps first n elements of each string
#'
#' @param x vector
#' @param split separator
#' @param as_numeric try to convert to numeric
#'
#' @return
#' @export
#'
#' @importFrom magrittr %>%
#'
strsplit_keep_last <- function(x, split, as_numeric = F) {
  
  strsplit_keep_lastn(x, split, n = 1, as_numeric = as_numeric)
  
}


#' Locates last position of pattern in given string
#'
#' @param string vector of strings
#' @param pattern pattern to match
#'
#' @return
#' @export
#'
#'
str_locate_last <- function(string, pattern) {
  
  pattern <- str_rev(pattern)
  
  pattern <- gsub(pattern = "\\.", replacement = "\\\\.", x = pattern)
  
  
  pos <- regexpr(pattern = pattern, text = str_rev(string))
  
  for (i in seq_along(pos)) {
    
    if (pos[i] != -1) pos[i] <- nchar(string)[i] - pos[i] + 1
    
  }
  
  
  return(pos)
  
}


#' Returns given strings in reverse order
#'
#' @param x character vector
#'
#' @return
#' @export
#'
#'
str_rev <- function(x) {
  
  x_rev <- sapply(X = x, FUN = function(y) {
    paste(rev(strsplit_(y, "")), collapse = "")
  }, USE.NAMES = F)
  
  return(x_rev)
  
}


