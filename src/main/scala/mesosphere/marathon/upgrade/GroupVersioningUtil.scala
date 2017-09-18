package mesosphere.marathon.upgrade

import mesosphere.marathon.state.{ AppDefinition, RootGroup, Timestamp, VersionInfo }
import org.slf4j.LoggerFactory

/**
  * app，group 版本工具类
  * Tools related to app/group versioning.
  */
object GroupVersioningUtil {
  private[this] val log = LoggerFactory.getLogger(getClass)

  /**
    * Calculate a new group from the given `to` parameter that sets the version of all changed apps
    * to the given `version`.
    *
    * @param version the version of all changed apps
    * @param from the original group
    * @param to the updated group
    * @return the updated group with updated app versions
    */
  def updateVersionInfoForChangedApps(version: Timestamp, from: RootGroup, to: RootGroup): RootGroup = {

    def updateAppVersionInfo(maybeOldApp: Option[AppDefinition], newApp: AppDefinition): AppDefinition = {
      val newVersionInfo = maybeOldApp match {
        case None =>
          //创建过程，第一次，会走这个分支的
          log.info(s"[${newApp.id}]: ------>GroupVersioningUtil.scala<-----new app detected")
          VersionInfo.forNewConfig(newVersion = version)
        case Some(oldApp) =>
          if (oldApp.isUpgrade(newApp)) {
            log.info(s"[${newApp.id}]: ------>GroupVersioningUtil.scala<-------upgrade detected for app (oldVersion ${oldApp.versionInfo})")
            oldApp.versionInfo.withConfigChange(newVersion = version)
          } else if (oldApp.isOnlyScaleChange(newApp)) {
            log.info(s"[${newApp.id}]: ------>GroupVersioningUtil.scala<-------scaling op detected for app (oldVersion ${oldApp.versionInfo})")
            oldApp.versionInfo.withScaleOrRestartChange(newVersion = version)
          } else if (oldApp.versionInfo != newApp.versionInfo && newApp.versionInfo == VersionInfo.NoVersion) {
            log.info(s"[${newApp.id}]: ------>GroupVersioningUtil.scala<-------restart detected for app (oldVersion ${oldApp.versionInfo})")
            oldApp.versionInfo.withScaleOrRestartChange(newVersion = version)
          } else {
            oldApp.versionInfo
          }
      }

      newApp.copy(versionInfo = newVersionInfo)
    }

    val originalApps = from.transitiveAppsById
    val updatedTargetApps = to.transitiveApps.flatMap { newApp =>
      val updated = updateAppVersionInfo(originalApps.get(newApp.id), newApp)
      if (updated.versionInfo != newApp.versionInfo) Some(updated) else None
    }
    updatedTargetApps.foldLeft(to) { (resultGroup, updatedApp) =>
      resultGroup.updateApp(updatedApp.id, _ => updatedApp, version)
    }
  }

}
