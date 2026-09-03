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
