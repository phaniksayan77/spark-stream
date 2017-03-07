trait Executor {
  var config: Map[String, String] = null
  var convertSourceToDataFrame: (scala.Any) => DataFrame
  var processDataFrame: (scala.Any) => DataFrame
  var processPartitions: (Iterator[Row] => Unit) = null;

  def init(input: Object) = {
    input.init()
  }

  protected def process(source: scala.Any): scala.Any = {
    val df = convertSourceToDataFrame(source)
    df.registerTempTable("sourceDF")
    
    if (!df.rdd.isEmpty) {
        processedData = processDataFrame(df)
        
        if (processPartitions != null) {
          try {
            processedData.foreachPartition(processPartitions)
          } catch {
            case e: Exception => {
              throw (e)
            }
          }
        }
      }
    } else {
      scala.None
    }
  }

  def run() {
    throw new RuntimeException("Method Not Implemented")
  }

  def run(source: String): scala.Any = {
    if (!source.isEmpty()) {
      process(source)
    } else {
      LOG.error(s"Source is Empty [$source]")
    }
  }
