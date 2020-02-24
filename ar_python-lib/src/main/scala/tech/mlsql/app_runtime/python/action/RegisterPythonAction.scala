package tech.mlsql.app_runtime.python.action

import tech.mlsql.app_runtime.commons.{Editor, FormParams, Input, KV}
import tech.mlsql.app_runtime.db.service.BasicDBService
import tech.mlsql.app_runtime.plugin.user.action.{BaseAction, CanAccess}
import tech.mlsql.app_runtime.python.PluginDB.ctx
import tech.mlsql.app_runtime.python.PluginDB.ctx._
import tech.mlsql.app_runtime.python.quill_model
import tech.mlsql.app_runtime.python.service.ArPythonService
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.serviceframework.platform.action.{ActionContext, CustomAction}
import tech.mlsql.serviceframework.platform.{PluginItem, PluginType}

class RegisterPythonAction extends BaseAction {
  override def _run(params: Map[String, String]): String = {
    val canAccess = if (BasicDBService.isDBSupport) {
      ArPythonService.checkLoginAndResourceAccess(
        ArPythonService.Config.actionResourceKey(RegisterPythonAction.action),
        params)
    } else CanAccess(BasicDBService.adminToken == params.getOrElse("admin_token", ""), "")


    if (!canAccess.access) {
      render(
        ActionContext.context().httpContext.response,
        400,
        JSONTool.toJsonStr(Map("msg" -> canAccess.msg)))
    }


    val name = params(RegisterPythonAction.Params.CODE_NAME.name)
    val code = params(RegisterPythonAction.Params.CODE.name)

    if (BasicDBService.isDBSupport) {
      def fetch = {
        ctx.run(
          ctx.query[quill_model.PythonScript].filter(_.name == lift(name))
        ).headOption
      }

      fetch match {
        case Some(_) => ctx.run(ctx.query[quill_model.PythonScript].filter(_.name == lift(name)).update(_.code -> lift(code)))
        case None => ctx.run(ctx.query[quill_model.PythonScript].insert(_.name -> lift(name), _.code -> lift(code)))
      }
      JSONTool.toJsonStr(List(fetch.get))
    } else {
      RegisterPythonAction.CODE_CACHE.put(name, code)
      JSONTool.toJsonStr(List(quill_model.PythonScript(-1, name, code)))
    }

  }


  override def _help(): String = {
    JSONTool.toJsonStr(FormParams.toForm(RegisterPythonAction.Params).toList.reverse)
  }
}

object RegisterPythonAction {

  object Params {

    val CODE_NAME = Input("codeName", "")

    val CODE = Editor("code", values = List(), valueProvider = Option(() => {
      List(
        KV(Option("python"), Option(""))
      )
    }))

    val ADMIN_TOKEN = Input("admin_token", "")
  }

  val CODE_CACHE = new java.util.concurrent.ConcurrentHashMap[String, String]()

  def action = "registerPyAction"

  def plugin = PluginItem(action, classOf[RegisterPythonAction].getName, PluginType.action, None)
}


