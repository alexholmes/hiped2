#! /usr/bin/env Rscript

library(Rhipe)

rhinit(TRUE,TRUE)

rhlapply(10000, function(r) paste(sample(letters[1:10],5),collapse=""),output.folder='/tmp/words')