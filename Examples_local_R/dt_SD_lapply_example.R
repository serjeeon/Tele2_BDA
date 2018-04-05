library(data.table)

dt <- data.table(
  X = sample(1:10),
  Y = sample(c("yes,asd", "n,o"), 10, replace = TRUE),
  Z = sample(c("ysu,as", "a,s,s"), 10, replace = T)
)

dt[, LETTERS[1:2] := lapply(.SD, function(x) gsub(",", "!", x)), .SDcols = c(2,3)]
head(dt)
