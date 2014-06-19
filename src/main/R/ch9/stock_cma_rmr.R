#! /usr/bin/env Rscript
library(rmr2)

map = function(k,v) {
  keyval(v[[1]], mean(as.double(c(v[[3]], v[[6]]))))
}

reduce = function(k,vv) {
  keyval(k, mean(as.numeric(unlist(vv))))
}

mapreduce(
  input = "/user/aholmes/stocks.txt",
  input.format=make.input.format("csv", sep = ","),
  #input.format= "text",
  output = "output",
  output.format = "text",
  map = map,
  reduce = reduce)
