package tech.mlsql.app_runtime.plugin

import tech.mlsql.app_runtime.python.action.{PythonAction, RegisterPythonAction}
import tech.mlsql.serviceframework.platform._

class PluginDesc extends Plugin {
  override def entries: List[PluginItem] = {
    List(
      PluginItem("pyAction", classOf[PythonAction].getName, PluginType.action, None),
      PluginItem("registerPyAction", classOf[RegisterPythonAction].getName, PluginType.action, None))
  }

  def registerForTest() = {
    val pluginLoader = PluginLoader(Thread.currentThread().getContextClassLoader, this)
    entries.foreach { item =>
      AppRuntimeStore.store.registerAction(item.name, item.clzzName, pluginLoader)
    }
  }
}
