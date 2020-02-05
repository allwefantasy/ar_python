package tech.mlsql.app_runtime.python.action

import tech.mlsql.app_runtime.db.action.BasicActionProxy
import tech.mlsql.app_runtime.python.PluginDB

object ArPythonProxy {
  lazy val proxy = new BasicActionProxy(PluginDB.plugin_name)
}
