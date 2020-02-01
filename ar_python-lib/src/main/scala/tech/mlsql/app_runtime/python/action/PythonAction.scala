package tech.mlsql.app_runtime.python.action

import java.util

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import tech.mlsql.app_runtime.python.PluginDB.ctx
import tech.mlsql.app_runtime.python.PluginDB.ctx._
import tech.mlsql.app_runtime.python.quill_model.PythonScript
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonConf, PythonFunction}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros.str
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.serviceframework.platform.action.CustomAction

import scala.collection.JavaConverters._

/**
 * 21/1/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class PythonAction extends CustomAction with Logging {
  override def run(params: Map[String, String]): String = {
    val env = params.getOrElse("env", "source activate streamingpro-spark-2.4.x") + " && export ARROW_PRE_0_15_IPC_FORMAT=1 "

    val envs = new util.HashMap[String, String]()
    envs.put(str(PythonConf.PYTHON_ENV), env)

    val pythonVersion = params.getOrElse("pythonVersion", "3.6")

    val code = params.get("codeName") match {
      case Some(codeName) =>
        ctx.run(ctx.query[PythonScript].filter(_.name == lift(codeName))).head.code
      case None => params.getOrElse("code",
        """
          |
          |""".stripMargin)
    }
    val inputSchema = StructType(Seq(StructField("key", StringType), StructField("value", StringType)))
    val enconder = RowEncoder.apply(inputSchema).resolveAndBind()

    //val outputSchema = StructType(Seq(StructField("value", StringType)))
    val pythonStartTime = System.currentTimeMillis()
    val batch = new ArrowPythonRunner(
      Seq(ChainedPythonFunctions(Seq(PythonFunction(
        code, envs, "python", pythonVersion)))), inputSchema,
      "GMT", Map()
    )
    val newIter = params.map(r => Row.fromSeq(Seq(r._1, r._2))).map { irow =>
      enconder.toRow(irow).copy()
    }.iterator
    val javaConext = new JavaContext
    val commonTaskContext = new AppContextImpl(javaConext, batch)
    val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
    val content = columnarBatchIter.flatMap { batch =>
      batch.rowIterator.asScala
    }.map(r => r.copy()).toList.head.getString(0)
    javaConext.markComplete
    javaConext.close
    logInfo(s"Python execute time:${System.currentTimeMillis() - pythonStartTime}")
    content
  }
}

