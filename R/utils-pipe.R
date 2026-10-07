#' Pipe operator
#'
#' See \code{magrittr::\link[magrittr:pipe]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom magrittr %>%
#' @usage lhs \%>\% rhs
#' @param lhs A value or the magrittr placeholder.
#' @param rhs A function call using the magrittr semantics.
#' @return The result of calling `rhs(lhs)`.
NULL


#' Returns the name of the lhs object copied from https://github.com/tidyverse/magrittr/issues/115#issuecomment-173894787
#'
#' @returns
#' @export
#'
#' @examples
.the_lhs <- function() {
  parents <- lapply(sys.frames(), parent.env)
  
  is_magrittr_env <-
    vapply(parents, identical, logical(1), y = environment(`%>%`))
  
  if (any(is_magrittr_env)) {
    llhs <- deparse(get("lhs", sys.frames()[[max(which(is_magrittr_env))]]))
    regmatches(llhs, regexpr("([^ ]+)", llhs, perl = T))
  }
}

