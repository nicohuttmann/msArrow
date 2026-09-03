# Small fixtures shared by the test files.

# Table that arrow can always infer a type for
test_table <- function(n = 3) {
  tibble::tibble(Protein.Group = paste0("P", seq_len(n)),
                 Genes = letters[seq_len(n)],
                 Intensity = seq_len(n) * 1.5,
                 n_peptides = seq_len(n))
}


# Long observations x variables frame
test_long <- function(n_obs = 4, n_var = 6) {
  tidyr::expand_grid(observations = paste0("run", seq_len(n_obs)),
                     variables = paste0("P", seq_len(n_var))) %>%
    dplyr::mutate(Intensity = seq_len(n_obs * n_var) * 1.5)
}
