#! /usr/bin/env Rscript

library(Rhipe)

source.data.file <- "/tmp/small-sample-tweets.dat.bz2"
output.folder <- "/tmp/rhipe-out"

setup.block <- list(
        map=expression({
                home.Rlib <- "/path/to/your/local/R/libraries"
                invisible( .libPaths( c( home.Rlib , .libPaths() ) ) )
                library(RJSONIO)
        }) ,
        reduce=expression({ })
)

config.block <- list(
        mapred.output.compress="true" ,
        mapred.output.compression.codec="org.apache.hadoop.io.compress.BZip2Codec" ,
        mapred.task.timeout=600000
)

map.block <- expression({

        rhcounter( "map_stage" , "enter_block" , 1 )

        map.function <- function( tweet.raw ){
                tryCatch({
                        tweet <- fromJSON( tweet.raw )
                        chars.in.tweet <- nchar( tweet$text )
                        rhcollect( tweet$user$screen_name , chars.in.tweet )
                        rhcounter( "map_stage" , "success" , 1 )
                } ,
                error=function( error ){
                        rhcounter( "map_stage" , "error" , 1 )
                        print( error )
                })
        }

        lapply( map.values , map.function )
})

reduce.block <- expression(
        pre = {
                tweetCount <- 0
                tweetLength <- 0
                currentKey <- reduce.key
                rhcounter( "reduce_stage" , "pre" , 1 )
        } ,
        reduce = {
                tweetCount <- tweetCount + length( reduce.values )
                tweetLength <- tweetLength + sum( unlist( reduce.values ) )
                rhcounter( "reduce_stage" , "reduce" , 1 )
        } ,
        post = {
                mean.length <- as.integer( round(tweetLength/tweetCount) )
                rhcollect( currentKey , mean.length )
                rhcounter( "reduce_stage" , "post" , 1 )
        }
)

rhinit(TRUE,TRUE,buglevel=2000)

options.block <- rhoptions()
options.block$runner[1] <- "/usr/local/lib/R/site-library/Rhipe/libs/imperious.so"

rhipe.job.def <- rhmr(
        jobname="rhipe example 2" ,

        setup=setup.block ,
        map=map.block ,
        reduce=reduce.block ,

        opts=options.block ,

        mapred=config.block ,

        ifolder=source.data.file ,
        ofolder=output.folder ,
        inout=c( "text" , "text" )
)

rhipe.job.result <- rhex( rhipe.job.def )

output.data <- rhread( paste( output.folder , "/part-*" , sep="" ) , type="text" )

library(plyr)

tweet.means <- mdply(
        output.data ,
        function( line ){
                line <- gsub( "\r$" , "" , output.data )
                tuple <- unlist( strsplit( line , "\t" ) )

                return( data.frame( tname=tuple[1] , tcount=as.integer(tuple[2]) ))
        },
        .expand=FALSE
)