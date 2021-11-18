createSankeyPlot <- function(
  categorizedRegimensInfo,
  cohortDefinitionId
) {
  library(dplyr)
  require(dplyr)
  df <- read.csv(categorizedRegimensInfo)
  df <- df[df$lineOfTherapy %in% c("1", "2", '3') &
             df$cohortDefinitionId == cohortDefinitionId, ]
  group_and_concat <- df %>%
    select(cohortDefinitionId, personId, lineOfTherapy,regimensCategories) %>%
    group_by(personId) %>%
    dplyr::mutate(all_lines = paste(lineOfTherapy, collapse = "|"),
                  all_regimens = paste(regimensCategories, collapse = "|"))
  head(group_and_concat)
  group_and_concat = group_and_concat[ c("cohortDefinitionId","personId",'all_lines','all_regimens')]
  group_and_concat <- distinct(group_and_concat)
  n_occur <- data.frame(table(group_and_concat$personId))
  n_occur[n_occur$Freq > 1,]

  library("plyr")
  t <- count(group_and_concat, 'all_regimens')
  t

  library(splitstackshape)
  line <- cSplit(t, "all_regimens", "|")
  line$all_regimens_2
  library(networkD3)
  library(dplyr)
  library(tidyr)
  library(data.table)
  df <- data.frame(
    LoT0 = c(''),
    LoT1 = line$all_regimens_1,
    LoT2 = line$all_regimens_2,
    LoT3 = line$all_regimens_3,
    VALUE = line$freq)
  s = sum(df$VALUE)
  links <-
    df %>%
    dplyr:: as_tibble() %>%
    dplyr:: mutate(row = row_number()) %>%
    pivot_longer(cols = c(-row, -VALUE),
                 names_to = 'column', values_to = 'source') %>%
    dplyr:: mutate(column = match(column, names(df))) %>%
    dplyr:: mutate(source = paste0(source, '__', column)) %>%
    dplyr:: group_by(row) %>%
    dplyr:: mutate(target = lead(source, order_by = column)) %>%
    drop_na(target, source) %>%
    dplyr:: group_by(source, target) %>%
    dplyr::summarise(value = sum(VALUE), .groups = 'drop')

  links <- as.data.frame(links)
  nodes <- data.frame(name = unique(c(links$source, links$target)))
  nodes = data.table(nodes)
  links = data.table(links)
  links$source <- match(links$source, nodes$name) - 1
  links$target <- match(links$target, nodes$name) - 1
  nodes$name <- sub('__[0-9]+$', '', nodes$name)
  txt <- links[, .(total = sum(value)), by=c('target')]
  txt2 <- links[, .(total2 = round(sum(value)/s*100),1), by=c('target')]
  nodes[txt$target+1L, name := paste0(name, ', n=', txt$total, ' (', txt2$total2,'%)')]
  links$type <- sub(' .*', '',
                    as.data.frame(nodes)[links$source + 1, 'name'])
  links
  setkey(links, type)
  links <- links[!""]
  return(sankeyNetwork(Links=links, Nodes=nodes, Source='source', Target='target',
                  Value='value', NodeID='name', fontSize=10, LinkGroup = 'type'))

}
