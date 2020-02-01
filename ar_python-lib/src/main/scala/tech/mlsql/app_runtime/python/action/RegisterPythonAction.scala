package tech.mlsql.app_runtime.python.action

import tech.mlsql.app_runtime.python.PluginDB.ctx
import tech.mlsql.app_runtime.python.PluginDB.ctx._
import tech.mlsql.app_runtime.python.quill_model
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.serviceframework.platform.action.CustomAction

class RegisterPythonAction extends CustomAction {
  override def run(params: Map[String, String]): String = {
    val name = params("name")
    val code = params("code")

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
  }
}


