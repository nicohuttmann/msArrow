# Helpers shared by the test files.
#
# The dataset store functions keep their registry in two globals, `Datasets`
# (previews) and `.Datasets` (paths), and write to them with `<<-`. Tests
# therefore have to create those globals and remove them again afterwards, or
# state leaks from one test file into the next.


# Small tibble that arrow can always infer a type for
test_table <- function(n = 3) {
  tibble::tibble(Protein.Group = paste0("P", seq_len(n)),
                 Genes = letters[seq_len(n)],
                 Intensity = seq_len(n) * 1.5,
                 n_peptides = seq_len(n))
}


# Long observations x variables frame as the store expects it
test_long <- function(n_obs = 4, n_var = 6) {
  tidyr::expand_grid(observations = paste0("run", seq_len(n_obs)),
                     variables = paste0("P", seq_len(n_var))) %>%
    dplyr::mutate(Intensity = seq_len(n_obs * n_var) * 1.5)
}


# Creates a dataset store in a temporary directory and removes the registry
# globals when the calling test or file finishes.
local_store <- function(dataset = "Precursors", env = parent.frame()) {

  dir <- withr::local_tempdir(.local_envir = env)

  msArrow::.add_dataset(dataset, save_dir = dir)

  withr::defer({
    suppressWarnings(rm(list = intersect(c("Datasets", ".Datasets", "Info"),
                                         ls(globalenv(), all.names = TRUE)),
                        envir = globalenv()))
  }, envir = env)

  dir
}


# Store with variables, observations and one data frame already saved
local_full_store <- function(dataset = "Precursors", env = parent.frame()) {

  dir <- local_store(dataset, env = env)

  suppressMessages(
    msArrow::save_variables_data(tibble::tibble(variables = paste0("P", 1:6),
                                                Genes = letters[1:6],
                                                Protein.Group = paste0("PG", 1:6)),
                                 dataset = dataset,
                                 save_dir = dir))

  suppressMessages(
    msArrow::save_observations_data(tibble::tibble(observations = paste0("run", 1:4),
                                                   Condition = c("ctrl", "ctrl",
                                                                 "t8", "t8")),
                                    dataset = dataset,
                                    save_dir = dir))

  suppressMessages(
    msArrow::save_data_frame(test_long(),
                             dataset = dataset,
                             name = "Intensity",
                             save_dir = dir))

  dir
}


# Minimal synthetic DIA-NN report, written as parquet
write_test_report <- function(path, n_prec = 6, runs = c("A", "B")) {

  report <- tidyr::expand_grid(Run = runs,
                               idx = seq_len(n_prec)) %>%
    dplyr::mutate(Precursor.Id = paste0("PEP", idx, "_2"),
                  Modified.Sequence = paste0("PEP", idx),
                  Stripped.Sequence = paste0("PEP", idx),
                  Protein.Group = paste0("PG", idx),
                  Protein.Ids = paste0("PG", idx),
                  Protein.Names = paste0("PROT", idx),
                  Genes = paste0("GENE", idx),
                  First.Protein.Description = paste0("description ", idx),
                  Proteotypic = 1,
                  Decoy = 0,
                  Precursor.Mz = 400 + idx,
                  Precursor.Charge = 2L,
                  Precursor.Quantity = idx * 100,
                  Precursor.Normalised = idx * 110,
                  Ms1.Area = idx * 90,
                  Ms1.Normalised = idx * 95,
                  Quantity.Quality = 0.9) %>%
    dplyr::select(-idx)

  arrow::write_parquet(report, path)

  path
}
