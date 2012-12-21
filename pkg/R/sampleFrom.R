#' Create resample from rhivetable
#' @export
#' @S3method createResample rhivetable
createResample.rhivetable <- function(X, y, y.bias,times,...) {
  lapply(seq_len(times),sampleFrom.rhivetable,X=X,y=y,y.bias=y.bias,...)
}

#' @export
#' @S3method createResample default
createResample.default <- caret::createResample

createResample <- function(X,...) {
  UseMethod("createResample")
}

#' @export
sampleFrom <- function(X,...) {
  UseMethod("sampleFrom")
}

#' Bootstrap sample from the given table to achieve the desired mixture of the target class
#' Any fold/partition information is removed as well, as long as generated random numbers
#' Samples are generated with replacement, meaning that some rows may occur more than once
#' in the sample.  Rather than duplicate rows, this is represented through the configurable
#' weight column, which contains the weight of each row in the bootstrapped sample.
#' @export
#' @S3method sampleFrom rhivetable
#' 
sampleFrom.rhivetable <- function(X, p = NULL, y=NULL,y.bias = 1,N.sample=NULL,
    weight.colname = "weight_", hivecon=rhive.options("connection"),...) {

  X.descr = describe(X,hivecon=hivecon)

  if (!is.null(p) && !is.null(N.sample)) {
    stop("Provide one of: p, N.sample")
  }
  withBias <- (!is.null(y) && !is.null(y.bias))
 
  X.descr.data <- X.descr[!(X.descr[,1] %in% c("fold__","partition__")),]

  if (withBias) {
    if (is.list(y.bias)) y.bias = data.frame(names(y.bias),
        as.numeric(unlist(y.bias)),stringsAsFactors=FALSE)
    if (is.vector(y.bias)) y.bias = t(matrix(y.bias,nrow=2))

    stmt = str_c("SELECT ",y,", COUNT(*) FROM ",X@name," GROUP BY ",y)
  }else {
    stmt = str_c("SELECT COUNT(*) FROM ",X@name)
  }
  result.counts <- getQuery(hivecon,stmt)
  
  N <- ifelse(withBias,
      sum(result.counts[,2]),
      result.counts[[1]])
  
  if (is.null(N.sample)) {
    N.sample <- p * N
  }else if (is.null(p)) {
    p <- N.sample/N
  }else {
    stop("This should be innaccessible")
  }

  sample_col = str_c("rbinom(",N.sample,",",1/N,")")

  if (withBias) {
    if (any(!(y.bias[,1] %in% result.counts[,1]))) {
      stop("Values provided in y.bias not found in the table")
    }
    has.bias <- (result.counts[,1] %in% y.bias[,1])
    bias.sum <- sum(y.bias[,2]) #this portion of sample is accounted for in y.bias
    if (bias.sum>1) {
      stop("Invalid y.bias: should indicate desired proportions in sample data")
    }
    bias.other <- 1 - bias.sum #this portion needs to be accounted for by the rest of y values
    n.other = 0.0 #number of 'other' values in the original data
    if (any(!has.bias)) {
      warning("Missing column values in y.bias: ",
          str_c(result.counts[!has.bias,1],collapse=", "))
      n.other <- sum(result.counts[!has.bias,2])
    }
    n.samples.other <- floor(bias.other * N.sample)
    p.samples.other <- 1/n.other

    sample_col = str_c("CASE ",y,"\n",
        str_c("WHEN ",y.bias[,1]," THEN rbinom(",
          floor(y.bias[,2]*N.sample),",",
          1/result.counts[match(y.bias[,1],result.counts[,1]),2],")",
          collapse="\n"),"\n",
        "ELSE rbinom(",n.samples.other,",",p.samples.other,")\n",
        "END")
  }

  stmt = str_c("SELECT *,\n",
      sample_col," AS sample__ \n",
       "FROM ",X@name)

  X.tmp <- hive.create(str_c(X@name,"_sample_tmp"),query=stmt,hivecon=hivecon,
      existing="rename")
 
  stmt = str_c("SELECT ",
         str_c_columns(X.descr.data[,1]," AS ",X.descr.data[,1],last=FALSE),
         "\tsample__ AS ",weight.colname,"\n",
       "FROM ",X.tmp@name,"\n",
       "WHERE sample__ > 0") 
  X.sample <- hive.create(str_c(X@name,"_sample"),query=stmt,hivecon=hivecon,...)
  hive.drop(X.tmp,hivecon=hivecon)
  X.sample
}

#' @export
setMethod("sampleFrom",list(X="rhivetable"),sampleFrom.rhivetable)
