package tech.mlsql.app_runtime.python.action

import tech.mlsql.app_runtime.plugin.user.action.{AddResourceToRoleOrUser, UserSystemActionProxy}
import tech.mlsql.app_runtime.python.action.PyAuthAction.Params
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.serviceframework.platform.action.{ActionContext, CustomAction}
import tech.mlsql.serviceframework.platform.{PluginItem, PluginType}

/**
 * 4/2/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class PyAuthAction extends CustomAction {
  override def run(params: Map[String, String]): String = {
    val canAccess = ArPythonService.checkLoginAndResourceAccess(ArPythonService.Config.adminResourceKey, params)
    if (!canAccess.access) {
      render(
        ActionContext.context().httpContext.response,
        400,
        JSONTool.toJsonStr(Map("msg" -> canAccess.msg)))
    }
    val resourceName = params(Params.RESOURCE_NAME)
    val newParams = params.get(Params.ROLE_NAME) match {
      case Some(roleName) =>
        Map(AddResourceToRoleOrUser.Params.ROLE_NAME -> roleName)
      case None =>
        Map(AddResourceToRoleOrUser.Params.AUTHORIZED_USER_NAME -> params(Params.AUTHORIZED_USER_NAME))
    }

    def addToDB(name: String) = {
      UserSystemActionProxy.proxy.run(AddResourceToRoleOrUser.action, params ++ Map(
        AddResourceToRoleOrUser.Params.RESOURCE_NAME -> name
      ) ++ newParams)
    }

    params(Params.RESOURCE_TYPE) match {
      case ArPythonService.ResourceType.ADMIN =>
        addToDB(ArPythonService.Config.adminResourceKey)

      case ArPythonService.ResourceType.CUSTOM =>
        addToDB(ArPythonService.Config.customResourceKey(resourceName))

      case ArPythonService.ResourceType.ACTION =>
        addToDB(ArPythonService.Config.actionResourceKey(resourceName))
    }
    JSONTool.toJsonStr(Map("msg" -> "success"))
  }
}

object PyAuthAction {

  object Params {
    val RESOURCE_TYPE = "resourceType"
    val RESOURCE_NAME = "resourceName"
    val ROLE_NAME = "roleName"
    val AUTHORIZED_USER_NAME = AddResourceToRoleOrUser.Params.AUTHORIZED_USER_NAME
  }

  def action = "pyAuthAction"

  def plugin = PluginItem(action, classOf[PyAuthAction].getName, PluginType.action, None)
}
