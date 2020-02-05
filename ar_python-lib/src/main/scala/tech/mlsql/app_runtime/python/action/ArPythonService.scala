package tech.mlsql.app_runtime.python.action

import tech.mlsql.app_runtime.plugin.user.action._
import tech.mlsql.app_runtime.python.PluginDB
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.serviceframework.platform.action.RenderFunctions

/**
 * 1/2/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object ArPythonService extends RenderFunctions {

  object ResourceType {
    val ACTION = "action"
    val CUSTOM = "custom"
    val ADMIN = "admin"
  }

  object Config {


    def actionResourceKey(name: String) = s"${PluginDB.plugin_name}__config__${ResourceType.ACTION}_access_${name}"

    def customResourceKey(name: String) = s"${PluginDB.plugin_name}__config__${ResourceType.CUSTOM}_access_${name}"

    val adminResourceKey = s"${PluginDB.plugin_name}__config__${ResourceType.ADMIN}_access"
  }

  def checkLoginAndResourceAccess(resource: String, params: Map[String, String]) = {
    val uapStr = UserSystemActionProxy.proxy.run(CheckAuthAction.action, params ++ Map(UserService.Config.RESOURCE_KEY -> resource))
    val canAccess = JSONTool.parseJson[CanAccess](uapStr)
    canAccess
  }

  def isLogin(userName: String, token: String) = {
    val isLoginStr = UserSystemActionProxy.proxy.run(IsLogin.action, Map(
      UserService.Config.USER_NAME -> userName,
      UserService.Config.LOGIN_TOKEN -> token
    ))
    JSONTool.parseJson[List[tech.mlsql.app_runtime.plugin.user.Session]](isLoginStr)
  }

}
