
#' @inheritParams caret::createFolds
#' @S3method createFolds default
#' @export
createFolds.default <- caret::createFolds

#' @export
createFolds <- function(X,y,...) {
  UseMethod("createFolds")
}

#' Create folds for k-fold cross-validation.
#' This only creates the folds in a duplicate data table.
#'
#' @param hivecon an 'rhive' jdbc connection to a hive server
#' @param X an 'rhive' reference to a hive table
#' @param y column(s) to use for stratified sampling
#' @param k number of partitions to create
#' @export
#' @import stringr rhive
createFolds.rhivetable <- function(X, y, k = 10,hivecon=rhive.options("connection"),name,...) {
    X.descr = describe(X,hivecon=hivecon)
    
    p <- seq(0,1,length=(k+1))[c(-1,-(k+1))]

    stmt = str_c("SELECT dat.*,rand() AS rand__ \n",
          "FROM ",X@name," dat")

    X.rand <- hive.create(name=str_c(X@name,"_rand"),
        query=stmt,external=FALSE,
        existing="rename",
        hivecon=hivecon)
    
    stmt <- str_c("SELECT ",y,", percentile_approx(",
        "rand__, array(",str_c(p,collapse=","),"))\n",
        "FROM ",X.rand@name,"\n",
        "GROUP BY ",y)
    
    message(stmt)
    X.rand.out <- getQuery(hivecon,stmt)

    y.rand.hists <- lapply(str_split(str_sub(X.rand.out[,2],2,-2),","),
        as.numeric)
    y.rand.names <- X.rand.out[,1]
    stmt <- str_c("SELECT ",
        str_c_columns("dat.",X.descr[,1]," AS ",X.descr[,1],last=FALSE),
        "\tget_cut(dat.rand__, CASE ",y,"\n",
        str_c("WHEN ",format(y.rand.names[-1]),
        " THEN array(",sapply(y.rand.hists[-1],str_c,collapse=","),")",collapse="\n"),"\n",
        " ELSE array(",str_c(y.rand.hists[[1]],collapse=","),")","\n",
        "END\n",
        ") AS fold__ \n",
        "FROM ",X.rand@name," dat")
    X.folds <- hive.create(
        name=if (missing(name)) { 
        str_c(X@name,"_folds")
        }else {
        name
        },
        query=stmt,hivecon=hivecon,...)
    hive.drop(X.rand,hivecon=hivecon)
    X.folds
}

#' @export
setMethod("createFolds",list(X="rhivetable",y="ANY"),
    createFolds.rhivetable)
