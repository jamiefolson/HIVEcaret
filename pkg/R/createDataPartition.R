
#' @inheritParams caret::createDataPartition
#' @S3method createDataPartition default
#' @export
createDataPartition.default <- caret::createDataPartition

#' @export
createDataPartition <- function(X,y,...) {
  UseMethod("createDataPartition")
}
setGeneric('createDataPartition',
    function(X,y,...)
      standardGeneric('createDataPartition')
    )

#' Data Splitting Functions
#' A series of test/training partitions are created using
#' ‘createDataPartition’ while ‘createResample’ creates one or more
#' bootstrap samples. ‘createFolds’ splits the data into ‘k’ groups.
#' 
#' Because these operations are performed on Hive tables, new hive 
#' tables are created and references returned, rather than actual samples
#'
#' @param hivecon an 'rhive' jdbc connection to a hive server
#' @param X an 'rhive' reference to a hive table
#' @param y column(s) to use for stratified sampling
#' @param times number of times to partition the data
#' @param groups number of groups to split y into for stratified sampling of a numeric value
#' @export
#' @import stringr rhive
createDataPartition.rhivetable <- function(X, y=NULL, times = 1, p = 0.5, 
    groups = min(5, length(y)),hivecon=rhive.options("connection"),name,...) {
#  if (!is.null(y) & hive.is.numeric(X=X,y=y)) {
#    stop("numeric columns not currently supported")
#  }
  if (!is.null(y)){
    if (is.character(y)) {
      y = as.quoted(y)
    }
    stmt = to_sql(X$select(.(`*`,as(rand(),rand__))))

    X.rand <- hive.create(name=str_c(X@name,"_rand"),
        query=stmt,existing="rename",
        hivecon=hivecon,external=FALSE)
    
    stmt <- to_sql(X.rand$select(from(X.rand,.(.(y),
            percentile_approx(rand__,.(p)))))$group_by(from(X.rand,y)))

    X.rand.out <- getQuery(hivecon,stmt)
    y.rand.cutoffs = X.rand.out[,2]
    y.rand.names = X.rand.out[,1]
    stmt = to_sql(X.rand$select(c(as.quoted(colnames(X)),
          partition__=.(rand__ < switch(.(y),
              .(as.quoted(y.rand.names[-1])),
              .(y.rand.cutoffs[-1]),
              .(y.rand.cutoffs[[1]]))))))
    name <- if(missing(name)) {
          str_c(X@name,"_partition")
        }else {
          name
        }
    X.sample <- hive.create(name=name,
        query=stmt,hivecon=hivecon,...)
    hive.drop(X.rand,hivecon=hivecon)
    X.sample
  }else {
    stop("I haven't done this yet")
  }
}

#' @export
setMethod("createDataPartition",list(X="rhivetable",y="ANY"),
    createDataPartition.rhivetable)

