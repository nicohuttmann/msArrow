#' Garbage collection function ruthlessly copied from https://stackoverflow.com/a/1467334
#'
#' @param n number of gc() iterations 
#'
#' @returns
#' @export
#'
#' 
cleanMem <- function(n = 10) { 
  for (i in 1:n) 
    gc() 
}


#' Remove objects from the global environment 
#'
#' @param exclude which objects not to remove
#'
#' @return
#' @export
#'
#' 
cleanup <- function(exclude = c()) {
  
  rm(list = setdiff(objects(name = globalenv()), c("Analysis",
                                                     "Datasets",
                                                     "Info", 
                                                     exclude)), 
       pos = globalenv())
  
  
  
  return(invisible("Good job."))
}


#' F(actors) U(nique) 
#'
#' @param x a vector
#'
#' @return
#' @export
#'
#' 
fu <- function (x) {
  factor(x = x, levels = unique(x))
}


#' Vector with elements as names
#'
#' @param ... 
#'
#' @return
#' @export
#'
#' 
cc <- function(...) {
  setNames(c(...), c(...))
}

