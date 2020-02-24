package tech.mlsql.app_runtime.python.action

import tech.mlsql.app_runtime.commons.{FormParams, Input, KV, Select}
import tech.mlsql.app_runtime.plugin.user.action.{AddResourceToRoleOrUser, BaseAction, UserSystemActionProxy}
import tech.mlsql.app_runtime.python.action.PyAuthAction.Params
import tech.mlsql.app_runtime.python.service.ArPythonService
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.serviceframework.platform.action.ActionContext
import tech.mlsql.serviceframework.platform.{PluginItem, PluginType}

/**
 * 4/2/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class PyAuthAction extends BaseAction {
  override def _run(params: Map[String, String]): String = {
    val canAccess = ArPythonService.checkLoginAndResourceAccess(ArPythonService.Config.adminResourceKey, params)
    if (!canAccess.access) {
      render(
        ActionContext.context().httpContext.response,
        400,
        JSONTool.toJsonStr(Map("msg" -> canAccess.msg)))
    }
    val resourceName = params(Params.RESOURCE_NAME.name)
    val newParams = params.get(Params.ROLE_NAME.name) match {
      case Some(roleName) =>
        Map(AddResourceToRoleOrUser.Params.ROLE_NAME.name -> roleName)
      case None =>
        Map(AddResourceToRoleOrUser.Params.AUTHORIZED_USER_NAME.name -> params(Params.AUTHORIZED_USER_NAME.name))
    }

    def addToDB(name: String) = {
      UserSystemActionProxy.proxy.run(AddResourceToRoleOrUser.action, params ++ Map(
        AddResourceToRoleOrUser.Params.RESOURCE_NAME.name -> name
      ) ++ newParams)
    }

    params(Params.RESOURCE_TYPE.name) match {
      case ArPythonService.ResourceType.ADMIN =>
        addToDB(ArPythonService.Config.adminResourceKey)

      case ArPythonService.ResourceType.CUSTOM =>
        addToDB(ArPythonService.Config.customResourceKey(resourceName))

      case ArPythonService.ResourceType.ACTION =>
        addToDB(ArPythonService.Config.actionResourceKey(resourceName))
    }
    JSONTool.toJsonStr(Map("msg" -> "success"))
  }

  override def _help(): String = {
    JSONTool.toJsonStr(FormParams.toForm(PyAuthAction.Params).toList.reverse)
  }
}

object PyAuthAction {

  object Params {
    val RESOURCE_TYPE = Select("resourceType", List(), valueProvider = Option(() => {
      List(
        KV(Option(ArPythonService.ResourceType.ADMIN), Option(ArPythonService.ResourceType.ADMIN)),
        KV(Option(ArPythonService.ResourceType.CUSTOM), Option(ArPythonService.ResourceType.CUSTOM)),
        KV(Option(ArPythonService.ResourceType.ACTION), Option(ArPythonService.ResourceType.ACTION))
      )
    }))

    val RESOURCE_NAME = Input("resourceName", "")
    val ROLE_NAME = Input("roleName", "")
    val AUTHORIZED_USER_NAME = Input(AddResourceToRoleOrUser.Params.AUTHORIZED_USER_NAME.name, "")

    val ADMIN_TOKEN = Input("admin_token", "")
  }

  def action = "pyAuthAction"

  def plugin = PluginItem(action, classOf[PyAuthAction].getName, PluginType.action, None)
}
