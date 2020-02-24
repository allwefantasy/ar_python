package tech.mlsql.app_runtime.plugin

import tech.mlsql.app_runtime.python.action.{PyAuthAction, PythonAction, PythonScriptAction, RegisterPythonAction}
import tech.mlsql.serviceframework.platform._

class PluginDesc extends Plugin {
  override def entries: List[PluginItem] = {
    List(
      PythonAction.plugin,
      RegisterPythonAction.plugin,
      PyAuthAction.plugin,
      PythonScriptAction.plugin
    )
  }

  def registerForTest() = {
    val pluginLoader = PluginLoader(Thread.currentThread().getContextClassLoader, this)
    entries.foreach { item =>
      AppRuntimeStore.store.registerAction(item.name, item.clzzName, pluginLoader)
    }
  }
}
