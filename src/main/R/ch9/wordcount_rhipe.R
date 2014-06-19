#! /usr/bin/env Rscript
library(Rhipe)

rhinit(TRUE,TRUE)

m <- expression({
  for(x in map.values){
    y <- strsplit(x," +")[[1]]
    for(w in y) rhcollect(w,T)
  }
})

z <- rhmr(map=m,inout=c("text","sequence"),
    ifolder="stocks.txt",ofolder='/output',mapred=list(mapred.reduce.tasks=5))

rhex(z)