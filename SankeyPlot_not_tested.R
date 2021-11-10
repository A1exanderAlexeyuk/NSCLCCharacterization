library(networkD3)
library(dplyr)
library(tidyr)
library(data.table)

df <- data.frame(
  LoT0 = c(''),
  LoT1 = c("x","y", "z", "t", "s", 's','s'),
  LoT2 = c("z", "z", "z", "s", "x",'null','x'),
  LoT3 = c("t", "z", "t", "t", "z",'x','t'),
  VALUE = c(1,3,8,10,15, 9,3))
s = sum(df$VALUE)

links <-
  df %>%
  as_tibble() %>%
  mutate(row = row_number()) %>%
  pivot_longer(cols = c(-row, -VALUE),
               names_to = 'column', values_to = 'source') %>%
  mutate(column = match(column, names(df))) %>%
  mutate(source = paste0(source, '__', column)) %>%
  group_by(row) %>%
  mutate(target = lead(source, order_by = column)) %>%
  drop_na(target, source) %>%
  group_by(source, target) %>%
  summarise(value = sum(VALUE), .groups = 'drop')

# convert tibble to dataframe
links <- as.data.frame(links)

nodes <- data.frame(name = unique(c(links$source, links$target)))
nodes = data.table(nodes)

links = data.table(links)

links$source <- match(links$source, nodes$name) - 1
links$target <- match(links$target, nodes$name) - 1

nodes$name <- sub('__[0-9]+$', '', nodes$name)

# ____ labeling the nodes
txt <- links[, .(total = sum(value)), by=c('target')]
txt2 <- links[, .(total2 = round(sum(value)/s*100)), by=c('target')]
nodes[txt$target+1L, name := paste0(name, ', n=', txt$total, ' (', txt2$total2,'%)')]

# ____ set color
links$type <- sub(' .*', '',
                  as.data.frame(nodes)[links$source + 1, 'name'])

#______remove first empty node
links
setkey(links, type)
links <- links[!""]

# Displays the counts as part of the labels
sankeyNetwork(Links=links, Nodes=nodes, Source='source', Target='target',
              Value='value', NodeID='name', fontSize=16, LinkGroup = 'type')
