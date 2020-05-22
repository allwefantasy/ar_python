package tech.mlsql.app_runtime.python.action

import java.util

import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import tech.mlsql.app_runtime.commons.{FormParams, Input, KV, Select}
import tech.mlsql.app_runtime.db.service.BasicDBService
import tech.mlsql.app_runtime.plugin.user.action.BaseAction
import tech.mlsql.app_runtime.python.PluginDB.ctx
import tech.mlsql.app_runtime.python.PluginDB.ctx._
import tech.mlsql.app_runtime.python.quill_model.PythonScript
import tech.mlsql.app_runtime.python.service.ArPythonService
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonConf, PythonFunction}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros.str
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.serviceframework.platform.{PluginItem, PluginType}

import scala.collection.JavaConverters._

/**
 * 21/1/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class PythonAction extends BaseAction with Logging {
  override def _run(params: Map[String, String]): String = {
    val codeName = params(PythonAction.Params.CODE_NAME.name)
    val code = if (BasicDBService.isDBSupport) {
      ctx.run(ctx.query[PythonScript].filter(_.name == lift(codeName))).head.code
    } else RegisterPythonAction.CODE_CACHE.get(codeName)
    
    val canAccess = ArPythonService.canAccess(ArPythonService.Config.customResourceKey(codeName), params)
    if (!canAccess.access) {
      render(400, JSONTool.toJsonStr(Map("msg" -> canAccess.msg)))
    }

    val env = params.getOrElse(PythonAction.Params.ENV.name, "source activate streamingpro-spark-2.4.x") + " && export ARROW_PRE_0_15_IPC_FORMAT=1 "

    val envs = new util.HashMap[String, String]()
    envs.put(str(PythonConf.PYTHON_ENV), env)

    val pythonVersion = params.getOrElse("pythonVersion", "3.6")


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
    val content = new JSONArray()
    columnarBatchIter.flatMap { batch =>
      batch.rowIterator.asScala
    }.map(r => r.copy()).toList.foreach(item => content.add(JSONObject.fromObject(item.getString(0))))
    javaConext.markComplete
    javaConext.close
    logInfo(s"Python execute time:${System.currentTimeMillis() - pythonStartTime}")
    content.toString()
  }

  override def _help(): String = {
    JSONTool.toJsonStr(FormParams.toForm(PythonAction.Params).toList.reverse)
  }
}

object PythonAction {

  object Params {
    val ENV = Input("env", "")

    val CODE_NAME = Select("codeName", List[KV](), valueProvider = Option(() => {
      val items = ctx.run(ctx.query[PythonScript]).map(item => item.name).map { item =>
        KV(Option(item), Option(item))
      }
      items
    }))
    val ADMIN_TOKEN = Input("admin_token", "")

  }

  def action = "pyAction"

  def plugin = PluginItem(action, classOf[PythonAction].getName, PluginType.action, None)
}








