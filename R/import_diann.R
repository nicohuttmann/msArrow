#' Title
#'
#' @param file 
#' @param name 
#' @param filter_by 
#' @param observation_names 
#' @param variables_data 
#' @param data_frames 
#' @param preview_precursors 
#' @param preview_format 
#' @param save_dir 
#' @param partition_by_run 
#' @param silent 
#'
#' @returns
#' @export
#'
#' @examples
import_diann <- function(file = "report.parquet", 
                         name = "Precursors", 
                         filter_by = Proteotypic == 1 & Decoy == 0, 
                         observation_names = list(pattern = ".+"), 
                         variables_data = "default", 
                         data_frames = "default", 
                         preview_precursors = c("all", "top100"), 
                         preview_format = c("wide_obs", "wide_vars", "long"), 
                         save_dir, 
                         partition_by_run = F, 
                         silent = F) {
  
  if (!silent) if (!silent) message("Step 1/5: Start")
  
  data_filtered <- arrow::open_dataset(file)
  
  # Defaults 
  
  # Variables data 
  if (all(variables_data %in% names(.get_defaults("variables_data")))) 
    variables_data <- .get_defaults("variables_data", variables_data) %>% 
    intersect(names(data_filtered))
  else 
    variables_data <- variables_data %>% 
    intersect(names(data_filtered))
  
  # Data frames 
  if (all(data_frames %in% names(.get_defaults("data_frames")))) 
    data_frames <- .get_defaults("data_frames", data_frames) %>% 
    intersect(names(data_filtered))
  else 
    data_frames <- data_frames %>% 
    intersect(names(data_filtered))
  
  
  if (!silent) message("Step 2/5: Filtering")
  
  # Prefilter data 
  if (partition_by_run) {
    message("Partitioning ", file, ' by "Run".')
    data_filtered <- data_filtered %>% 
      write_data(partitioning = "Run") %>% 
      open_data()
  }
  
  
  all_cols <- c("Run", 
                "Precursor.Id", 
                variables_data, 
                data_frames, 
                names(data_filtered)[
                  stringr::str_detect(paste(rlang::as_label(dplyr::enquo(filter_by)), 
                                            collapse = ""), 
                                      names(data_filtered))])
  
  data_filtered <- data_filtered %>% 
    dplyr::select(dplyr::all_of(all_cols)) %>% 
    dplyr::filter({{ filter_by }}) %>% 
    dplyr::compute()
  
  
  if (!silent) message("Step 3/5: Extracting precursors and runs")
  
  # Variables data
  variables_data_frame <- data_filtered %>% 
    dplyr::select(dplyr::any_of(c("variables" = "Precursor.Id", 
                                  variables_data))) %>% 
    dplyr::distinct() %>% 
    dplyr::collect()
  
  # Check row number 
  nrow_precursors <- data_filtered %>% 
    dplyr::distinct(Precursor.Id) %>% 
    dplyr::collect() %>% 
    nrow()
  
  # Check if number of precursor rows changes based on variables data columns 
  if (nrow(variables_data_frame) != nrow_precursors) {
    warning("The number of precursor rows changed from ", 
            nrow_precursors, 
            " to ", 
            nrow(variables_data_frame), 
            ". Please check the <variables_data> argument.")
  }
  
  # Extract precursor preview data 
  if (preview_precursors[1] == "all")
    variables_data_frame_preview <- variables_data_frame
  else if (preview_precursors[1] == "top100")
    variables_data_frame_preview <- variables_data_frame %>% 
    dplyr::slice_head(n = 100) 
  else 
    variables_data_frame_preview <- variables_data_frame %>% 
    dplyr::filter(Precursor.Id %in% preview_precursors)
  
  # Precursors to preview in data frames 
  vars <- variables_data_frame_preview %>% 
    dplyr::pull(variables)
  
  
  
  # Extract names 
  if (!is.list(observation_names) || 
      names(observation_names)[1] != "pattern" || 
      "replace" %in% names(observation_names)) {
    
    warning("Sample names must be declared by a list with the arguments <pattern> and (optional) <replace>.")
    
    observations_data <- data_filtered %>% 
      dplyr::distinct(Run) %>% 
      dplyr::mutate(observations = Run, .before = 1)
    
    
    # Rename based on pattern 
  } else {
    
    # 
    observations_data <- data_filtered %>% 
      dplyr::distinct(Run) %>% 
      dplyr::collect() %>% 
      dplyr::mutate(observations = 
                      stringr::str_extract(Run, 
                                           observation_names$pattern), 
                    .before = 1) 
    
    # Replace matched names by observation_names$replace
    if ("replace" %in% names(observation_names)) 
      observations_data <- observations_data %>% 
        dplyr::mutate(observations = 
                        observation_names$replace[observations]) %>% 
        dplyr::arrange(observations)
    
  }
  
  
  if (!silent) message("Step 4/5: Extracting data frames")
  
  # Data_frames 
  data_frames_preview <- dplyr::inner_join(arrow::arrow_table(observations_data), 
                                           data_filtered %>% 
                                             dplyr::select(all_of(c("Run", 
                                                                    "variables" = "Precursor.Id", 
                                                                    data_frames))) %>% 
                                             dplyr::inner_join(variables_data_frame_preview, 
                                                               by = "variables"), 
                                           by = "Run") %>% 
    dplyr::select(dplyr::all_of(c("observations", 
                                  "variables", 
                                  data_frames))) %>% 
    dplyr::collect()
  
  
  data_filtered_c <- dplyr::inner_join(arrow::arrow_table(observations_data), 
                                       data_filtered %>% 
                                         dplyr::select(dplyr::all_of(c("Run", 
                                                                       "variables" = "Precursor.Id", 
                                                                       data_frames))), 
                                       by = "Run") %>% 
    dplyr::select(dplyr::all_of(c("observations", 
                                  "variables", 
                                  data_frames))) %>% 
    dplyr::compute()
  
  
  
  if (!silent) message("Step 5/5: Assembling dataset and saving")
  
  # Assemble background 
  if (!exists(".Datasets", where = globalenv())) 
    assign(".Datasets", list(), pos = globalenv())
  
  .Datasets[[name]] <<- list(Variables = c(), 
                             Observations = c(), 
                             Data_frames = list())
  
  .Datasets[[name]][["Variables"]] <<- write_data(variables_data_frame, 
                                                  file = "Variables", 
                                                  dir = file.path(save_dir, name), 
                                                  silent = F)
  
  .Datasets[[name]][["Observations"]] <<- write_data(observations_data, 
                                                     file = "Observations", 
                                                     dir = file.path(save_dir, name), 
                                                     silent = F)
  
  
  for (df in data_frames) {
    
    if (!partition_by_run) 
      .Datasets[[name]][["Data_frames"]][[df]] <<- 
        write_data(x = data_filtered_c %>% 
                     dplyr::select(dplyr::all_of(c("observations", 
                                                   "variables", 
                                                   df))), 
                   file = stringr::str_replace_all(df, "\\.", "_"), 
                   dir = file.path(save_dir, name, "Data_frames"), 
                   silent = F)
    
    else
      .Datasets[[name]][["Data_frames"]][[df]] <<- 
        write_data(x = data_filtered_c %>% 
                     dplyr::select(dplyr::all_of(c("observations", 
                                                   "variables", 
                                                   df))), 
                   file = stringr::str_replace_all(df, "\\.", "_"), 
                   dir = file.path(save_dir, name, "Data_frames"), 
                   partitioning = "observations", silent = F)
    
  }
  
  
  # Assemble preview data 
  list_preview <- 
    list(Variables = variables_data_frame_preview, 
         Observations = observations_data, 
         Data_frames = 
           purrr::map(cc(data_frames), \(df) {
             data_frames_preview %>% 
               {if (match.arg(preview_format, c("wide_obs", 
                                                "wide_vars", 
                                                "long")) == "wide_obs")
                 tidyr::pivot_wider(., 
                                    id_cols = "observations", 
                                    names_from = "variables", 
                                    values_from = df)
                 else if (match.arg(preview_format, c("wide_obs", 
                                                      "wide_vars", 
                                                      "long")) == "wide_vars")
                   tidyr::pivot_wider(., 
                                      id_cols = "variables", 
                                      names_from = "observations", 
                                      values_from = df)
                 else if (match.arg(preview_format, c("wide_obs", 
                                                      "wide_vars", 
                                                      "long")) == "long")
                   dplyr::select(., dplyr::all_of(c("observations", 
                                                    "variables", 
                                                    df)))
                 else {
                   warning('preview_format not supported. Using default "wide_obs".')
                   tidyr::pivot_wider(., 
                                      id_cols = "observations", 
                                      names_from = "variables", 
                                      values_from = df)
                 }}}
           ))
  
  message("Done.")
  
  return(list_preview)
  
}


#' Title
#'
#' @param file 
#' @param name 
#' @param channel 
#' @param filter_by 
#' @param observation_names 
#' @param variables_data 
#' @param data_frames 
#' @param preview_precursors 
#' @param preview_format 
#' @param save_dir 
#' @param partition_by_run 
#'
#' @returns
#' @export
#'
#' @examples
import_diann_channel <- function(file = "report.parquet", 
                                 name = "Precursors", 
                                 channel = "Channel", 
                                 filter_by = Proteotypic == 1 & Decoy == 0, 
                                 observation_names = list(pattern = ".+"), 
                                 variables_data = "default", 
                                 data_frames = "default", 
                                 preview_precursors = c("all", "top100"), 
                                 preview_format = c("wide_obs", "wide_vars", "long"), 
                                 save_dir, 
                                 partition_by_run = F, 
                                 silent = F) {
  
  
  if (!silent) message("Step 1/5: Start")
  
  data_filtered <- arrow::open_dataset(file)
  
  # Check if channel label was added multiple times 
  if (data_filtered %>% 
      dplyr::filter(Channel != "") %>% 
      dplyr::pull(Precursor.Id, as_vector = TRUE) %>% 
      head(10000) %>% 
      stringr::str_detect(paste0("(\\(", channel, "\\)){2,}")) %>% 
      any()) {
    
    if (!silent) message("Note: Multiple channel labels per residue, correcting to only one.") 
    
    channel_n <- paste0("(\\(", channel, "\\))+")
    channel_1 <- paste0("(", channel, ")")
    
    data_filtered <- data_filtered %>% 
      dplyr::mutate(Precursor.Id = stringr::str_replace_all(
        Precursor.Id, channel_n, channel_1)) 
    
  } else if (data_filtered %>% 
             dplyr::filter(Channel != "") %>% 
             dplyr::pull(Precursor.Id, as_vector = TRUE) %>% 
             head(10000) %>% 
             stringr::str_detect(paste0("\\(", channel, "\\)"), negate = T) %>% 
             all()) {
    stop("Channel modification ", channel, " was not found in the data.")
  }
  
  
  # Defaults 
  
  # Variables data 
  if (all(variables_data %in% names(.get_defaults("variables_data")))) {
    variables_data <- c("Channel", 
                        .get_defaults("variables_data", variables_data)) %>% 
      intersect(names(data_filtered))
  } else {
    variables_data <- c("Channel", variables_data) %>% 
      intersect(names(data_filtered))
  }
  
  # Data frames 
  if (all(data_frames %in% names(.get_defaults("data_frames")))) {
    data_frames <- .get_defaults("data_frames", data_frames) %>% 
      intersect(names(data_filtered))
  } else {
    data_frames <- data_frames %>% 
      intersect(names(data_filtered))
  }
  
  
  if (!silent) message("Step 2/5: Filtering")
  
  all_cols <- c("Run", 
                "Precursor.Id", 
                variables_data, 
                data_frames, 
                names(data_filtered)[
                  stringr::str_detect(paste(rlang::as_label(dplyr::enquo(filter_by)), 
                                            collapse = ""), 
                                      names(data_filtered))])
  
  data_filtered <- data_filtered %>% 
    dplyr::select(dplyr::all_of(all_cols)) %>% 
    dplyr::filter({{ filter_by }}) %>% 
    dplyr::compute()
  
  
  if (!silent) message("Step 2.5/5: Modifying Precursor Ids")
  
  # Add channel to Precursor.Id
  data_filtered <- data_filtered %>% 
    dplyr::distinct(Precursor.Id, Channel) %>% 
    dplyr::collect() %>% 
    split(.$Channel) %>% 
    purrr::imap(\(x, i) x %>% 
                  dplyr::mutate(Precursor_Id = 
                                  stringr::str_replace_all(Precursor.Id, 
                                                           channel, 
                                                           paste0(channel, "-", i)))) %>% 
    dplyr::bind_rows() %>% 
    arrow::arrow_table() %>% 
    dplyr::right_join(data_filtered, 
                      by = c("Precursor.Id", "Channel")) %>% 
    dplyr::mutate(Precursor.Id = Precursor_Id) %>% 
    dplyr::select(-Precursor_Id) %>% 
    dplyr::arrange(Modified.Sequence) %>% 
    dplyr::compute()
  
  
  if (!silent) message("Step 3/5: Extracting precursors and runs")
  
  # Variables data
  variables_data_frame <- data_filtered %>% 
    dplyr::select(dplyr::any_of(c("variables" = "Precursor.Id", 
                                  variables_data))) %>% 
    dplyr::distinct() %>% 
    dplyr::collect() %>% 
    dplyr::mutate(Precursor.Channel.Group = 
                    stringr::str_remove_all(variables, 
                                            paste0("\\(", channel, "-.\\)")), 
                  Modified.Sequence = 
                    stringr::str_sub(variables, 
                                     1, 
                                     nchar(variables) - 1), 
                  PTM.Modified.Sequence = 
                    stringr::str_remove_all(Modified.Sequence, 
                                            paste0("\\(", channel, "-.\\)")), 
                  .after = "Channel") 
  
  # Check row number 
  nrow_precursors <- data_filtered %>% 
    dplyr::distinct(Precursor.Id) %>% 
    dplyr::collect() %>% 
    nrow()
  
  # Check if number of precursor rows changes based on variables data columns 
  if (nrow(variables_data_frame) != nrow_precursors) {
    warning("The number of precursor rows changed from ", 
            nrow_precursors, 
            " to ", 
            nrow(variables_data_frame), 
            ". Please check the <variables_data> argument.")
  }
  
  # Extract precursor preview data 
  if (preview_precursors[1] == "all") {
    variables_data_frame_preview <- variables_data_frame
  } else if (preview_precursors[1] == "top100") {
    variables_data_frame_preview <- variables_data_frame %>% 
      dplyr::slice_head(n = 100) 
  } else {
    variables_data_frame_preview <- variables_data_frame %>% 
      dplyr::filter(Precursor.Id %in% preview_precursors)
  }
  
  # Precursors to preview in data frames 
  vars <- variables_data_frame_preview %>% 
    dplyr::pull(variables)
  
  
  
  # Extract names 
  if (!is.list(observation_names) || 
      names(observation_names)[1] != "pattern" || 
      "replace" %in% names(observation_names)) {
    
    warning("Sample names must be declared by a list with the arguments <pattern> and (optional) <replace>.")
    
    observations_data <- data_filtered %>% 
      dplyr::distinct(Run) %>% 
      dplyr::mutate(observations = Run, .before = 1)
    
    
    # Rename based on pattern 
  } else {
    
    # 
    observations_data <- data_filtered %>% 
      dplyr::distinct(Run) %>% 
      dplyr::collect() %>% 
      dplyr::mutate(observations = 
                      stringr::str_extract(Run, 
                                           observation_names$pattern), 
                    .before = 1) 
    
    # Replace matched names by observation_names$replace
    if ("replace" %in% names(observation_names)) 
      observations_data <- observations_data %>% 
        dplyr::mutate(observations = 
                        observation_names$replace[observations]) %>% 
        dplyr::arrange(observations)
    
  }
  
  
  if (!silent) message("Step 4/5: Extracting data frames")
  
  # Data_frames 
  data_frames_preview <- dplyr::inner_join(arrow::arrow_table(observations_data), 
                                           data_filtered %>% 
                                             dplyr::select(all_of(c("Run", 
                                                                    "variables" = "Precursor.Id", 
                                                                    data_frames))) %>% 
                                             dplyr::inner_join(variables_data_frame_preview, 
                                                               by = "variables"), 
                                           by = "Run") %>% 
    dplyr::select(-Run) %>% 
    dplyr::collect()
  
  
  data_filtered_c <- dplyr::inner_join(arrow::arrow_table(observations_data), 
                                       data_filtered %>% 
                                         dplyr::select(all_of(c("Run", 
                                                                "variables" = "Precursor.Id", 
                                                                data_frames))), 
                                       by = "Run") %>% 
    dplyr::select(-Run) %>% 
    dplyr::compute()
  
  
  
  if (!silent) message("Step 5/5: Assembling dataset and saving")
  
  # Assemble background 
  .add_dataset(name = name, save_dir = save_dir)
  
  .save_variables_data(variables_data_frame, 
                       variables_data_frame_preview = variables_data_frame_preview, 
                       dataset = name, 
                       save_dir = save_dir)
  
  .save_observations_data(observations_data, 
                          observations_data, 
                          dataset = name, 
                          save_dir = save_dir) 
  
  for (df in data_frames) {
    
    .save_data_frame(data_frame = data_filtered_c %>% 
                       dplyr::select(dplyr::all_of(c("observations", 
                                                     "variables", 
                                                     df))), 
                     data_frame_preview = data_frames_preview %>% 
                       {if (match.arg(preview_format, c("wide_obs", 
                                                        "wide_vars", 
                                                        "long")) == "wide_obs")
                         tidyr::pivot_wider(., 
                                            id_cols = "observations", 
                                            names_from = "variables", 
                                            values_from = df)
                         else if (match.arg(preview_format, c("wide_obs", 
                                                              "wide_vars", 
                                                              "long")) == "wide_vars")
                           tidyr::pivot_wider(., 
                                              id_cols = "variables", 
                                              names_from = "observations", 
                                              values_from = df)
                         else if (match.arg(preview_format, c("wide_obs", 
                                                              "wide_vars", 
                                                              "long")) == "long")
                           dplyr::select(., dplyr::all_of(c("observations", 
                                                            "variables", 
                                                            df)))
                         else {
                           warning('preview_format not supported. Using default "wide_obs".')
                           tidyr::pivot_wider(., 
                                              id_cols = "observations", 
                                              names_from = "variables", 
                                              values_from = df)
                         }}, 
                     dataset = name, 
                     name = df, 
                     save_dir = save_dir, 
                     partitioning = ifelse(partition_by_run, 
                                           "observations", 
                                           "donotdoit"), 
                     silent = F)
    
  }
  
  
  # # Assemble preview data 
  # list_preview <- 
  #   list(Variables = variables_data_frame_preview, 
  #        Observations = observations_data, 
  #        Data_frames = 
  #          purrr::map(cc(data_frames), \(df) {
  #            data_frames_preview %>% 
  #              {if (match.arg(preview_format, c("wide_obs", 
  #                                               "wide_vars", 
  #                                               "long")) == "wide_obs")
  #                tidyr::pivot_wider(., 
  #                                   id_cols = "observations", 
  #                                   names_from = "variables", 
  #                                   values_from = df)
  #                else if (match.arg(preview_format, c("wide_obs", 
  #                                                     "wide_vars", 
  #                                                     "long")) == "wide_vars")
  #                  tidyr::pivot_wider(., 
  #                                     id_cols = "variables", 
  #                                     names_from = "observations", 
  #                                     values_from = df)
  #                else if (match.arg(preview_format, c("wide_obs", 
  #                                                     "wide_vars", 
  #                                                     "long")) == "long")
  #                  dplyr::select(., dplyr::all_of(c("observations", 
  #                                                   "variables", 
  #                                                   df)))
  #                else {
  #                  warning('preview_format not supported. Using default "wide_obs".')
  #                  tidyr::pivot_wider(., 
  #                                     id_cols = "observations", 
  #                                     names_from = "variables", 
  #                                     values_from = df)
  #                }}}
  #          ))
  
  message("Done.")
  
  return(invisible(list(preview = .Datasets[[name]], 
                        locations = Datasets[[name]])))
  
}


#' Title
#'
#' @param file 
#'
#' @returns
#' @export
#'
#' @examples
check_report <- function(file) {
  
  message('Summarizing file: \n"', 
          file, 
          '"')
  
  list_stats <- list()
  
  data_file <- arrow::open_dataset(file)
  
  
  cat("\nNumber of Runs: ")
  
  list_stats$n_runs <- data_file %>% 
    dplyr::distinct(Run) %>% 
    dplyr::collect() %>% 
    nrow()
  
  cat(list_stats$n_runs, 
      "\nNumber of Precursors: ")
  
  list_stats$n_precursors <- data_file %>% 
    dplyr::distinct(Precursor.Id) %>% 
    dplyr::collect() %>% 
    nrow() 
  
  cat(list_stats$n_precursors, 
      "\nPrecursors / Run (ave.): ")
  
  list_stats$n_precursors_ave <- data_file %>% 
    dplyr::distinct(Precursor.Id, Run) %>% 
    dplyr::count(Run) %>% 
    dplyr::summarise(mean = mean(n, na.rm = T)) %>% 
    dplyr::collect() %>% 
    dplyr::pull(mean)
  
  cat(round(list_stats$n_precursors_ave), 
      "\nTotal Precursor completeleness: ")
  
  list_stats$p_precursor_completeleness <- 
    (data_file %>% 
       dplyr::distinct(Precursor.Id, Run) %>% 
       dplyr::collect() %>%
       nrow()) / (list_stats$n_runs * list_stats$n_precursors)
  
  cat(scales::percent(round(list_stats$p_precursor_completeleness, 3), 0.1), 
      "\n\nProtein groups: ")
  
  list_stats$n_proteins <- data_file %>% 
    dplyr::distinct(Protein.Group) %>% 
    dplyr::collect() %>% 
    nrow()
  
  cat(list_stats$n_proteins, 
      "\nProtein groups (min. 2 peptides): ")
  
  list_stats$n_proteins_2p <- data_file %>% 
    dplyr::distinct(Protein.Group, Stripped.Sequence) %>% 
    dplyr::count(Protein.Group) %>% 
    dplyr::filter(n >= 2) %>% 
    dplyr::collect() %>% 
    nrow()
  
  cat(round(list_stats$n_proteins_2p), 
      "\nProtein groups / Run (ave.): ")
  
  list_stats$n_proteins_ave <- data_file %>% 
    dplyr::distinct(Protein.Group, Run) %>% 
    dplyr::count(Run) %>% 
    dplyr::summarise(mean = mean(n, na.rm = T)) %>% 
    dplyr::collect() %>% 
    dplyr::pull(mean)
  
  cat(round(list_stats$n_proteins_ave), 
      "\nProtein groups (min. 2 peptides) / Run (ave.): ")
  
  list_stats$n_proteins_2p_ave <- data_file %>% 
    dplyr::distinct(Protein.Group, Run, Stripped.Sequence) %>% 
    dplyr::count(Protein.Group, Run) %>% 
    dplyr::filter(n >= 2) %>% 
    dplyr::count(Run) %>% 
    dplyr::summarise(mean = mean(n, na.rm = T)) %>% 
    dplyr::collect() %>% 
    dplyr::pull(mean)
  
  list_stats$obsevations <- data_file %>% 
    dplyr::distinct(Run) %>% 
    dplyr::collect() %>% 
    dplyr::pull(Run)
  
  list_stats$schema <- arrow::schema(data_file)
  
  
  cat(round(list_stats$n_proteins_2p_ave), "\n ")
  
  
  message("Returning list with additional information.")
  
  return(invisible(list_stats))
  
}


#' Title
#'
#' @returns
#' @export
#'
#' @examples
.add_defaults <- function() {
  
  if (!exists(".Info", where = globalenv())) .Info <<- list()
  
  if (is.null(.Info$defaults)) .Info$defaults <<- list()
  
  defaults <- list(variables_data = 
                     list(default = c("Modified.Sequence",
                                      "Stripped.Sequence",
                                      "Protein.Group",
                                      "Protein.Ids",
                                      "Protein.Names",
                                      "Genes",
                                      "First.Protein.Description",
                                      "Proteotypic",
                                      "Precursor.Mz",
                                      "Precursor.Charge"), 
                          SILAC = c("Channel", 
                                    "Modified.Sequence",
                                    "Stripped.Sequence",
                                    "Protein.Group",
                                    "Protein.Ids",
                                    "Protein.Names",
                                    "Genes",
                                    "First.Protein.Description",
                                    "Proteotypic",
                                    "Precursor.Mz",
                                    "Precursor.Charge"), 
                          default1 = c("Modified.Sequence",
                                       "Stripped.Sequence",
                                       "Protein.Group",
                                       "Protein.Names",
                                       "Genes",
                                       "First.Protein.Description")), 
                   data_frames = list(default = c("Precursor.Normalised", 
                                                  "Ms1.Normalised"), 
                                      default1 = c("Precursor.Quantity", 
                                                   "Precursor.Normalised", 
                                                   "Ms1.Area", 
                                                   "Ms1.Normalised"), 
                                      comprehensive = c("Precursor.Quantity", 
                                                        "Precursor.Normalised", 
                                                        "Ms1.Area", 
                                                        "Ms1.Normalised", 
                                                        "Quantity.Quality")))
  
  purrr::iwalk(defaults, 
               \(x, i) 
               if (!i %in% names(.Info$defaults))
                 .Info$defaults[[i]] <<- x)
  
}

#' Title
#'
#' @param type 
#' @param name 
#'
#' @returns
#' @export
#'
#' @examples
.get_defaults <- function(type, name) {
  
  .add_defaults()
  
  if (!hasArg(type)) {
    x <- .Info$defaults
  } else {
    x <- .Info$defaults[[type]]
  }
  
  if (hasArg(name)) 
    x <- x[[name]]
  
  return(x)
  
}
