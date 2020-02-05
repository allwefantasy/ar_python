package tech.mlsql.app_runtime.python.action

import tech.mlsql.app_runtime.db.service.BasicDBService
import tech.mlsql.app_runtime.plugin.user.action.CanAccess
import tech.mlsql.app_runtime.python.PluginDB.ctx
import tech.mlsql.app_runtime.python.PluginDB.ctx._
import tech.mlsql.app_runtime.python.quill_model
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.serviceframework.platform.action.{ActionContext, CustomAction}
import tech.mlsql.serviceframework.platform.{PluginItem, PluginType}

class RegisterPythonAction extends CustomAction {
  override def run(params: Map[String, String]): String = {
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


    val name = params("codeName")
    val code = params("code")

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
}

object RegisterPythonAction {
  val CODE_CACHE = new java.util.concurrent.ConcurrentHashMap[String, String]()

  def action = "registerPyAction"

  def plugin = PluginItem(action, classOf[RegisterPythonAction].getName, PluginType.action, None)
}


