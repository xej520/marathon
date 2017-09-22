package mesosphere.marathon
package api.v2

import java.net.URI
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, MediaType, Response }

import akka.event.EventStream
import com.codahale.metrics.annotation.Timed
import mesosphere.marathon.api.v2.Validation._
import mesosphere.marathon.api.v2.json.AppUpdate
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.api.{ AuthResource, MarathonMediaType, PATCH, RestResource }
import mesosphere.marathon.core.appinfo.{ AppInfo, AppInfoService, AppSelector, Selector, TaskCounts }
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.event.ApiPostEvent
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state._
import mesosphere.marathon.stream._
import play.api.libs.json.{ JsObject, Json }

@Path("v2/apps")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class AppsResource @Inject() (
    clock: Clock,
    eventBus: EventStream,
    appTasksRes: AppTasksResource,
    service: MarathonSchedulerService,
    appInfoService: AppInfoService,
    val config: MarathonConf,
    groupManager: GroupManager,
    pluginManager: PluginManager)(implicit
  val authenticator: Authenticator,
    val authorizer: Authorizer) extends RestResource with AuthResource {

  import AppsResource._

  private[this] val ListApps = """^((?:.+/)|)\*$""".r
  implicit lazy val appDefinitionValidator = AppDefinition.validAppDefinition(config.availableFeatures)(pluginManager)
  implicit lazy val appUpdateValidator = AppUpdate.appUpdateValidator(config.availableFeatures)

  @GET
  @Timed
  def index(
    @QueryParam("cmd") cmd: String,
    @QueryParam("id") id: String,
    @QueryParam("label") label: String,
    @QueryParam("embed") embed: java.util.Set[String],
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val selector = selectAuthorized(search(Option(cmd), Option(id), Option(label)))
    // additional embeds are deprecated!
    val resolvedEmbed = InfoEmbedResolver.resolveApp(embed) +
      AppInfo.Embed.Counts + AppInfo.Embed.Deployments
    val mapped = result(appInfoService.selectAppsBy(selector, resolvedEmbed))
    Response.ok(jsonObjString("apps" -> mapped)).build()
  }

  //标准化app
  def normalizedApp(appDef: AppDefinition, now: Timestamp): AppDefinition = {
    appDef.copy(
      ipAddress = appDef.ipAddress.map { ipAddress =>
        config.defaultNetworkName.get.collect {
          case (defaultName: String) if defaultName.nonEmpty && ipAddress.networkName.isEmpty =>
            ipAddress.copy(networkName = Some(defaultName))
        }.getOrElse(ipAddress)
      },
      versionInfo = VersionInfo.OnlyVersion(now)//就是时间戳
    )
  }

  @POST
  @Timed
  def create(
    body: Array[Byte],
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    withValid(Json.parse(body).as[AppDefinition].withCanonizedIds()) { appDef =>

      //创建一个版本号，其实，就是当前时间
      val now = clock.now()
      println("--------------------------------xej--------------------create--------------------")
      //app 的类型是  AppDefinition
      val app = normalizedApp(appDef, now)
      checkAuthorization(CreateRunSpec, app)
      def createOrThrow(opt: Option[AppDefinition]) = opt
        .map(_ => throw ConflictingChangeException(s"An app with id [${app.id}] already exists."))
        .getOrElse(app)

      println("---------<<AppsResource.scala>>-----------------result()-------start-----------------------")
      //result 实际上，调用的是Await.result 阻塞的方式 去获取Future[T]的值
      //根据appID 以及appVersion生成一个DeploymentPlan
      //plan  类型就是  DeploymentPlan
      //异步 +  阻塞的方式  去获取plan
      //Deployment Plan 阶段，主要完成的工作：
      //1、确定task部署在哪个节点上，
      //2、已经初始化号taskID, 端口号
      //Deployment是阻塞模式，完成不了，不会进入下一个阶段
      val plan = result(groupManager.updateApp(app.id, createOrThrow, app.version, force))
      println("---------<<AppsResource.scala>>-----------------result()-------end-----------------------")
      println("---------<<AppsResource.scala>>------appWithDeployments-----------AppInfo()-------start-----------------------")
      //你是不是傻，下面的形式，不就是创建一个
      // AppInfo对象么
      //很明显，一个app对应一个plan.id
      val appWithDeployments = AppInfo(
        app,
        //这些都是初始值啊
        maybeCounts = Some(TaskCounts.zero),
        maybeTasks = Some(Seq.empty),
        maybeDeployments = Some(Seq(Identifiable(plan.id)))
      )
      println("---------<<AppsResource.scala>>-------maybePostEvent()-------start-----------------------")
      maybePostEvent(req, appWithDeployments.app)
      println("---------<<AppsResource.scala>>-------maybePostEvent()-------end-------------------------")
      println("---------<<AppsResource.scala>>-------appWithDeployments.app-------2-------:\n" + jsonString(appWithDeployments.app))
      //  {
      //      "id":"/ftp/lgy002b",
      //      "cmd":"while [ true ] ; do echo 'Hello Marathon, Hello spark' ; sleep 5 ; done",
      //      "args":null,
      //      "user":null,
      //      "env":{},
      //      "instances":1,
      //      "cpus":0.1,
      //      "mem":128,
      //      "disk":0,
      //      "gpus":0,
      //      "executor":"",
      //      "constraints":[],
      //      "uris":[],
      //      "fetch":[],
      //      "storeUrls":[],
      //      "backoffSeconds":1,
      //      "backoffFactor":1.15,
      //      "maxLaunchDelaySeconds":3600,
      //      "container":{"type":"MESOS","volumes":[{"containerPath":"ftp_data","mode":"RW","persistent":{"size":10,"type":"root","constraints":[]}}]},
      //      "healthChecks":[],
      //      "readinessChecks":[],
      //      "dependencies":[],
      //      "upgradeStrategy":{"minimumHealthCapacity":0,"maximumOverCapacity":0},
      //      "labels":{},
      //      "ipAddress":null,
      //      "version":"2017-08-29T01:02:58.569Z",
      //      "residency":{"relaunchEscalationTimeoutSeconds":10,"taskLostBehavior":"WAIT_FOREVER"},
      //      "secrets":{},
      //      "taskKillGracePeriodSeconds":null,
      //      "unreachableStrategy":"disabled",
      //      "killSelection":"YOUNGEST_FIRST",
      //      "ports":[10034],
      //      "portDefinitions":[{"port":10034,"protocol":"tcp","labels":{}}],
      //      "requirePorts":false}
      //Response  这应该是java代码了
      Response
        .created(new URI(app.id.toString))
        .header(RestResource.DeploymentHeader, plan.id)
        .entity(jsonString(appWithDeployments))
        .build()
    }
  }

  @GET
  @Path("""{id:.+}""")
  @Timed
  def show(
    @PathParam("id") id: String,
    @QueryParam("embed") embed: java.util.Set[String],
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val resolvedEmbed = InfoEmbedResolver.resolveApp(embed) ++ Set(
      // deprecated. For compatibility.
      AppInfo.Embed.Counts, AppInfo.Embed.Tasks, AppInfo.Embed.LastTaskFailure, AppInfo.Embed.Deployments
    )

    def transitiveApps(groupId: PathId): Response = {
      result(groupManager.group(groupId)) match {
        case Some(group) =>
          checkAuthorization(ViewGroup, group)
          val appsWithTasks = result(appInfoService.selectAppsInGroup(groupId, authzSelector, resolvedEmbed))
          println("---<AppsResource.scala>--定时校验----大概--每5秒钟---就会执行---一次--appsWithTasks------:\t" + appsWithTasks)
          ok(jsonObjString("*" -> appsWithTasks))
        case None =>
          unknownGroup(groupId)
      }
    }

    def app(appId: PathId): Response = {
      result(appInfoService.selectApp(appId, authzSelector, resolvedEmbed)) match {
        case Some(appInfo) =>
          checkAuthorization(ViewRunSpec, appInfo.app)
          println("----<AppsResource.scala>----appInfo.app.taskSize------:\t" + appInfo.app.instances)
          ok(jsonObjString("app" -> appInfo))
        case None => unknownApp(appId)
      }
    }

    id match {
      case ListApps(gid) => {
        println("---<AppsResource.scala>--定时校验----大概--每5秒钟---就会执行---一次-----：列出当前App------")
        transitiveApps(gid.toRootPath)
      }
      case _ => app(id.toRootPath)
    }
  }

  /**
    * Validate and normalize a single application update submitted via the REST API. Validation exceptions are not
    * handled here, that's left as an exercise for the caller.
    *
    * @param appId used as the id of the generated app update (vs. whatever might be in the JSON body)
    * @param body is the raw, unparsed JSON
    * @param partialUpdate true if the JSON should be parsed as a partial application update (all fields optional)
    *                      or as a wholesale replacement (parsed like an app definition would be)
    */
  def canonicalAppUpdateFromJson(appId: PathId, body: Array[Byte], partialUpdate: Boolean): AppUpdate = {
    if (partialUpdate) {
      validateOrThrow(Json.parse(body).as[AppUpdate].copy(id = Some(appId)).withCanonizedIds())
    } else {
      // this is a complete replacement of the app as we know it, so parse and normalize as if we're dealing
      // with a brand new app because the rules are different (for example, many fields are non-optional with brand-new apps).
      // however since this is an update, the user isn't required to specify an ID as part of the definition so we do
      // some hackery here to pass initial JSON parsing.
      val jsObj = Json.parse(body).as[JsObject] + ("id" -> Json.toJson(appId.toString))
      // the version is thrown away in toUpdate so just pass `zero` for now
      normalizedApp(validateOrThrow(jsObj.as[AppDefinition].withCanonizedIds()), Timestamp.zero).toUpdate
    }
  }

  /**
    * Validate and normalize an array of application updates submitted via the REST API. Validation exceptions are not
    * handled here, that's left as an exercise for the caller.
    *
    * @param body is the raw, unparsed JSON
    * @param partialUpdate true if the JSON should be parsed as a partial application update (all fields optional)
    *                      or as a wholesale replacement (parsed like an app definition would be)
    */
  def canonicalAppUpdatesFromJson(body: Array[Byte], partialUpdate: Boolean): Seq[AppUpdate] = {
    if (partialUpdate) {
      validateOrThrow(Json.parse(body).as[Seq[AppUpdate]].map(_.withCanonizedIds()))
    } else {
      // this is a complete replacement of the app as we know it, so parse and normalize as if we're dealing
      // with a brand new app because the rules are different (for example, many fields are non-optional with brand-new apps).
      // the version is thrown away in toUpdate so just pass `zero` for now.
      validateOrThrow(Json.parse(body).as[Seq[AppDefinition]].map(_.withCanonizedIds())).map { app =>
        normalizedApp(app, Timestamp.zero).toUpdate
      }
    }
  }

  @PUT
  @Path("""{id:.+}""")
  @Timed
  def replace(
    @PathParam("id") id: String,
    body: Array[Byte],
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @DefaultValue("true")@QueryParam("partialUpdate") partialUpdate: Boolean,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    update(id, body, force, partialUpdate, req, allowCreation = true)
  }

  @PATCH
  @Path("""{id:.+}""")
  @Timed
  def patch(
    @PathParam("id") id: String,
    body: Array[Byte],
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    update(id, body, force, partialUpdate = true, req, allowCreation = false)
  }

  @PUT
  @Timed
  def replaceMultiple(
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @DefaultValue("true")@QueryParam("partialUpdate") partialUpdate: Boolean,
    body: Array[Byte],
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    updateMultiple(force, partialUpdate, body, allowCreation = true)
  }

  @PATCH
  @Timed
  def patchMultiple(
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    body: Array[Byte],
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    updateMultiple(force, partialUpdate = true, body, allowCreation = false)
  }

  @DELETE
  @Path("""{id:.+}""")
  @Timed
  def delete(
    @DefaultValue("true")@QueryParam("force") force: Boolean,
    @PathParam("id") id: String,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val appId = id.toRootPath

    def deleteApp(rootGroup: RootGroup) = {
      checkAuthorization(DeleteRunSpec, rootGroup.app(appId), AppNotFoundException(appId))
      rootGroup.removeApp(appId)
    }

    deploymentResult(result(groupManager.updateRoot(deleteApp, force = force)))
  }

  @Path("{appId:.+}/tasks")
  def appTasksResource(): AppTasksResource = appTasksRes

  @Path("{appId:.+}/versions")
  def appVersionsResource(): AppVersionsResource = new AppVersionsResource(service, groupManager, authenticator,
    authorizer, config)

  @POST
  @Path("{id:.+}/restart")
  def restart(
    @PathParam("id") id: String,
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val appId = id.toRootPath

    def markForRestartingOrThrow(opt: Option[AppDefinition]) = {
      opt
        .map(checkAuthorization(UpdateRunSpec, _))
        .map(_.markedForRestarting)
        .getOrElse(throw AppNotFoundException(appId))
    }

    val newVersion = clock.now()
    val restartDeployment = result(
      groupManager.updateApp(id.toRootPath, markForRestartingOrThrow, newVersion, force)
    )

    deploymentResult(restartDeployment)
  }

  /**
    * Internal representation of `replace or update` logic.
    *
    * @param id appId
    * @param body request body
    * @param force force update?
    * @param partialUpdate partial update?
    * @param req http servlet request
    * @param allowCreation is creation allowed?
    * @param identity implicit identity
    * @return http servlet response
    */
  private[this] def update(id: String, body: Array[Byte], force: Boolean, partialUpdate: Boolean,
    req: HttpServletRequest, allowCreation: Boolean)(implicit identity: Identity): Response = {
    val appId = id.toRootPath

    assumeValid {
      val appUpdate = canonicalAppUpdateFromJson(appId, body, partialUpdate)
      val version = clock.now()
      val plan = result(groupManager.updateApp(appId, updateOrCreate(appId, _, appUpdate, partialUpdate, allowCreation), version, force))
      val response = plan.original.app(appId)
        .map(_ => Response.ok())
        .getOrElse(Response.created(new URI(appId.toString)))
      plan.target.app(appId).foreach { appDef =>
        maybePostEvent(req, appDef)
      }
      deploymentResult(plan, response)
    }
  }

  /**
    * Internal representation of `replace or update` logic for multiple apps.
    *
    * @param force force update?
    * @param partialUpdate partial update?
    * @param body request body
    * @param allowCreation is creation allowed?
    * @param identity implicit identity
    * @return http servlet response
    */
  private[this] def updateMultiple(force: Boolean, partialUpdate: Boolean,
    body: Array[Byte], allowCreation: Boolean)(implicit identity: Identity): Response = {

    assumeValid {
      val version = clock.now()
      val updates = canonicalAppUpdatesFromJson(body, partialUpdate)

      def updateGroup(rootGroup: RootGroup): RootGroup = updates.foldLeft(rootGroup) { (group, update) =>
        update.id match {
          case Some(id) => group.updateApp(id, updateOrCreate(id, _, update, partialUpdate, allowCreation = allowCreation), version)
          case None => group
        }
      }

      deploymentResult(result(groupManager.updateRoot(updateGroup, version, force)))
    }
  }

  private[v2] def updateOrCreate(
    appId: PathId,
    existing: Option[AppDefinition],
    appUpdate: AppUpdate,
    partialUpdate: Boolean,
    allowCreation: Boolean)(implicit identity: Identity): AppDefinition = {
    def createApp(): AppDefinition = {
      val app = validateOrThrow(appUpdate.empty(appId))
      checkAuthorization(CreateRunSpec, app)
    }

    def updateApp(current: AppDefinition): AppDefinition = {
      val updatedApp = if (partialUpdate) appUpdate(current) else appUpdate.empty(appId)
      val validatedApp = validateOrThrow(updatedApp)
      checkAuthorization(UpdateRunSpec, validatedApp)
    }

    def rollback(current: AppDefinition, version: Timestamp): AppDefinition = {
      val app = service.getApp(appId, version).getOrElse(throw AppNotFoundException(appId))
      checkAuthorization(ViewRunSpec, app)
      checkAuthorization(UpdateRunSpec, current)
      app
    }

    def updateOrRollback(current: AppDefinition): AppDefinition = appUpdate.version
      .map(rollback(current, _))
      .getOrElse(updateApp(current))

    existing match {
      case Some(app) =>
        // we can only rollback existing apps because we deleted all old versions when dropping an app
        updateOrRollback(app)
      case None if allowCreation =>
        createApp()
      case None =>
        throw AppNotFoundException(appId)
    }
  }

  private def maybePostEvent(req: HttpServletRequest, app: AppDefinition) = {
    //getRemoteAddr 远端地址，应该就是，客户端申请地址，如物理机
    println("-------<AppsResources.scala>------------req.getRemoteAddr------1--------:\n" + req.getRemoteAddr)
    //restfull 请求，就是marathonAPI的/v2/apps
    println("-------<AppsResources.scala>----------------req.getRequestURI------2--------:\n" + req.getRequestURI)
    println("-------<AppsResources.scala>---------------post提交事件--------------:\n" + ApiPostEvent(req.getRemoteAddr, req.getRequestURI, app))
    //    ApiPostEvent(
    // 172.16.91.77,/v2/apps,
    // AppDefinition(/ftp/lgy002b,Some(while [ true ] ; do echo 'Hello Marathon, Hello spark' ; sleep 5 ; done),List(),None,Map(),1,Resources(0.1,128.0,0.0,0),,Set(),List(),List(),List(PortDefinition(10034,tcp,None,Map())),false,BackoffStrategy(1 second,3600 seconds,1.15),Some(Mesos(List(PersistentVolume(ftp_data,PersistentVolumeInfo(10,None,root,Set()),RW)))),Set(),List(),None,Set(),UpgradeStrategy(0.0,0.0),Map(),Set(),None,OnlyVersion(2017-08-29T01:02:58.569Z),Some(Residency(10,WAIT_FOREVER)),Map(),UnreachableDisabled,YoungestFirst),api_post_event,2017-08-29T01:02:58.975Z)

    //很明显，下面又是创建了一的对象实例啊ApiPostEvent
    val apiPostEvent = ApiPostEvent(req.getRemoteAddr, req.getRequestURI, app)

    //EventStream 类似于 消息中间件
    //可以发布和接收消息
    //消息订阅者，必须是Actor才可以的
    //这句话的意思是，发布一条消息ApiPostEvent
    eventBus.publish(apiPostEvent)

    println("-------------------post提交事件---------结束-----------------------------------------------------------")
  }



  private[v2] def search(cmd: Option[String], id: Option[String], label: Option[String]): AppSelector = {
    def containCaseInsensitive(a: String, b: String): Boolean = b.toLowerCase contains a.toLowerCase
    val selectors = Seq[Option[Selector[AppDefinition]]](
      cmd.map(c => Selector(_.cmd.exists(containCaseInsensitive(c, _)))),
      id.map(s => Selector(app => containCaseInsensitive(s, app.id.toString))),
      label.map(new LabelSelectorParsers().parsed)
    ).flatten
    Selector.forall(selectors)
  }

  def selectAuthorized(fn: => AppSelector)(implicit identity: Identity): AppSelector = {
    Selector.forall(Seq(authzSelector, fn))
  }
}

object AppsResource {

  def authzSelector(implicit authz: Authorizer, identity: Identity): AppSelector = Selector[AppDefinition] { app =>
    authz.isAuthorized(identity, ViewRunSpec, app)
  }
}
