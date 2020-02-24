package tech.mlsql.app_runtime.python.action

import tech.mlsql.app_runtime.commons.{FormParams, Input, KV, Select}
import tech.mlsql.app_runtime.plugin.user.action.BaseAction
import tech.mlsql.app_runtime.python.PluginDB.ctx
import tech.mlsql.app_runtime.python.PluginDB.ctx._
import tech.mlsql.app_runtime.python.quill_model.PythonScript
import tech.mlsql.app_runtime.python.service.ArPythonService
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.serviceframework.platform.action.ActionContext
import tech.mlsql.serviceframework.platform.{PluginItem, PluginType}

class PythonScriptAction extends BaseAction with Logging {


  def delete(name: String) = {
    ctx.run(ctx.query[PythonScript].filter(_.name == lift(name)).delete)
  }

  def list = {
    val scripts = ctx.run(ctx.query[PythonScript]).toList
    JSONTool.toJsonStr(scripts)
  }

  override def _run(params: Map[String, String]): String = {
    val canAccess = ArPythonService.canAccess(
      ArPythonService.Config.customResourceKey(PythonScriptAction.Config.RESOURCE_KEY), params)

    if (!canAccess.access) {
      render(
        ActionContext.context().httpContext.response,
        400,
        JSONTool.toJsonStr(Map("msg" -> canAccess.msg)))
    }

    params.get(PythonScriptAction.Params.OPERATE.name) match {
      case Some(PythonScriptAction.OperateValues.LIST) => list
      case Some(PythonScriptAction.OperateValues.DELETE) =>
        delete(params(PythonScriptAction.Params.codeName.name))
        list
      case None => list
    }

  }

  override def _help(): String = {
    JSONTool.toJsonStr(FormParams.toForm(PythonScriptAction.Params).toList.reverse)
  }
}

object PythonScriptAction {

  object Params {
    val codeName = Input("codeName", "")
    
    val OPERATE = Select("operate", List(), valueProvider = Option(() => {
      List(
        KV(Option(PythonScriptAction.OperateValues.LIST), Option(PythonScriptAction.OperateValues.LIST)),
        KV(Option(PythonScriptAction.OperateValues.DELETE), Option(PythonScriptAction.OperateValues.DELETE))
      )
    }))

  }

  object OperateValues {
    val LIST = "list"
    val DELETE = "delete"
  }

  object Config {
    val RESOURCE_KEY = "web"
  }

  def action = "pyScriptAction"

  def plugin = PluginItem(action, classOf[PythonScriptAction].getName, PluginType.action, None)
}