#' @export
getPartition <- function(X,...) {
  UseMethod("getPartition")
}



#' Bootstrap sample from the given table to achieve the desired mixture of the target class
#' Any fold/partition information is removed as well, as long as generated random numbers
#' Samples are generated with replacement, meaning that some rows may occur more than once
#' in the sample.  Rather than duplicate rows, this is represented through the configurable
#' weight column, which contains the weight of each row in the bootstrapped sample.
#' @param type one of c("train","test")
#' @export
#' @S3method getPartition rhivetable
#' 
getPartition.rhivetable <- function(X, type = "train",fold=NULL, hivecon=rhive.options("connection"),...) {

  X.descr = describe(X,hivecon=hivecon)
  X.type = NULL
  if ("fold__" %in% X.descr[,1]) {
    X.type = "folds"
    if (is.null(fold)) {
      stop("Must specify the fold for constructing train or test sets from folds")
    }
  }
  if ("partition__" %in% X.descr[,1]) {
    X.type = "partition"
  }
  if (is.null(X.type)) {
    stop("You must first createFolds or createDataPartition")
  }

  X.descr.data <- X.descr[!(X.descr[,1] %in% c("fold__","partition__")),]

  stmt = str_c("SELECT \n\t",
        str_c_columns(X.descr.data[,1]," AS ",X.descr.data[,1]),
        " FROM ",X@name,"\n",switch(X.type,
           data=switch(type,
             all="",
             train=stop("First create partition or fold tables"),
             test=stop("First create partition or fold tables")),
           partition=switch(type,
             all="",
             train=" WHERE partition__ ",
             test=" WHERE ! partition__ "),
           folds=switch(type,
             all="",
             train=str_c(" WHERE fold__ != ",fold," "),
             test=str_c(" WHERE fold__ == ",fold," "))))

  X.partition <- hive.create(str_c(X@name,"_subset_",type),query=stmt,
        hivecon=hivecon,...)
  X.partition
}

#' @export
setMethod("getPartition","rhivetable",getPartition.rhivetable)
