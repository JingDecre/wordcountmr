# wordcountmr
第一个MapReduce程序

1、用maven打包成jar包
2、上传到hadoop的服务器上
3、切换到jar包所在的文件夹，然后执行hadoop命令：
   hadoop jar word-count-mr-1.0.jar com.decre.hadoop.frdCountMR /input /output
   注意： “/input” 是集群上所需要处理文件的hdfs路径， “/output”是处理完结果文件放置的hdfs路径， 两者可以根据用户需要自定义修改
          
