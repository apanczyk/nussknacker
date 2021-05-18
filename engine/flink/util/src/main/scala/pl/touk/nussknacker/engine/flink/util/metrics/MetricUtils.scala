package pl.touk.nussknacker.engine.flink.util.metrics

import cats.data.NonEmptyList
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.metrics.{Counter, Gauge, Histogram, HistogramStatistics, Meter, MetricGroup}
import pl.touk.nussknacker.engine.flink.api.{NkGlobalParameters, RuntimeContextLifecycle}

/*
  IMPORTANT: PLEASE keep Metrics.md up to date

  Handling Flink metrics is a bit tricky. For long time we parsed tags directly in Influx, via graphite plugin
  This is complex and error prone, so we'd like to switch to passing flink metric variables as tags via native influx API
  Unfortunately, current Flink Influx report doesn't allow for elastic configuration, so for now
  we translate by default to `old` way in groupsWithNameForLegacyMode
 */
class MetricUtils(runtimeContext: RuntimeContext) {

  /**
    * Enabling this flag will cause all metrics to be registered under both old and new name.
    * As Flink's reporters deduplicate metric instances (see [[org.apache.flink.metrics.reporter.AbstractReporter]])
    * we will register some metrics using read-only proxies that report values of original metrics.
    */
  private val useLegacyMetricsMode: Boolean =
    NkGlobalParameters.readFromContext(runtimeContext.getExecutionConfig).flatMap(_.configParameters.flatMap(_.useLegacyMetrics)).getOrElse(false)

  def counter(nameParts: NonEmptyList[String], tags: Map[String, String]): Counter = {
    val (group, name) = groupsWithName(nameParts, tags)
    val counter = group.counter(name)

    if (useLegacyMetricsMode) {
      val (groupOld, nameOld) = groupsWithNameForLegacyMode(nameParts, tags)
      groupOld.counter(nameOld, new CounterReadProxy(counter))
    }

    counter
  }

  def gauge[T, Y <: Gauge[T]](nameParts: NonEmptyList[String], tags: Map[String, String], gauge: Y): Y = {
    val (group, name) = groupsWithName(nameParts, tags)
    group.gauge[T, Y](name, gauge)

    if (useLegacyMetricsMode) {
      val (groupOld, nameOld) = groupsWithNameForLegacyMode(nameParts, tags)
      groupOld.gauge[T, Y](nameOld, new GaugeProxy[T](gauge).asInstanceOf[Y])
    }

    gauge
  }

  //currently not used - maybe we should? :)
  def meter(nameParts: NonEmptyList[String], tags: Map[String, String], meter: Meter): Meter = {
    val (group, name) = groupsWithName(nameParts, tags)
    group.meter(name, meter)

    if (useLegacyMetricsMode) {
      val (groupOld, nameOld) = groupsWithNameForLegacyMode(nameParts, tags)
      groupOld.meter(nameOld, new MeterReadProxy(meter))
    }

    meter
  }

  def histogram(nameParts: NonEmptyList[String], tags: Map[String, String], histogram: Histogram): Histogram = {
    val (group, name) = groupsWithName(nameParts, tags)
    group.histogram(name, histogram)

    if (useLegacyMetricsMode) {
      val (groupOld, nameOld) = groupsWithNameForLegacyMode(nameParts, tags)
      groupOld.histogram(nameOld, new HistogramReadProxy(histogram))
    }

    histogram
  }

  private def groupsWithName(nameParts: NonEmptyList[String], tags: Map[String, String]): (MetricGroup, String) = {
    val namespaceTags = extractTags(NkGlobalParameters.readFromContext(runtimeContext.getExecutionConfig))
    tagMode(nameParts, tags ++ namespaceTags, false)
  }

  private def tagMode(nameParts: NonEmptyList[String], tags: Map[String, String], legacyMode: Boolean): (MetricGroup, String) = {
    val lastName = if (legacyMode) s"${nameParts.last}_nk_legacy" else s"${nameParts.last}_nk_new"

    //all but last
    val metricNameParts = nameParts.init
    val groupWithNameParts = metricNameParts.foldLeft(runtimeContext.getMetricGroup)(_.addGroup(_))

    val finalGroup = tags.toList.sortBy(_._1).foldLeft(groupWithNameParts) {
      case (group, (tag, tagValue)) => group.addGroup(tag, tagValue)
    }

    (finalGroup, lastName)
  }

  private def groupsWithNameForLegacyMode(nameParts: NonEmptyList[String], tags: Map[String, String]): (MetricGroup, String) = {
    def insertTag(tagId: String)(nameParts: NonEmptyList[String]): (MetricGroup, String) =
      tagMode(NonEmptyList(nameParts.head, tags(tagId)::nameParts.tail), Map.empty, true)

    val insertNodeId = insertTag("nodeId") _

    nameParts match {

      //RateMeterFunction, nodeId tag
      case l@NonEmptyList("source", _) => insertNodeId(l)
      //EventTimeDelayMeterFunction, nodeId tag
      case l@NonEmptyList("eventtimedelay", _) => insertNodeId(l)

      //EndRateMeterFunction, nodeId tag
      case l@NonEmptyList("end", _) => insertNodeId(l)
      case l@NonEmptyList("dead_end", _) => insertNodeId(l)

      //NodeCountMetricListener nodeId tag
      case l@NonEmptyList("nodeCount", _) =>insertNodeId(l)

      //GenericTimeMeasuringService
      case l@NonEmptyList("service", name :: "instantRate" :: Nil) => tagMode(NonEmptyList("serviceInstant",  tags("serviceName") :: name :: Nil), Map.empty, true)
      case l@NonEmptyList("service", name :: "histogram" :: Nil) => tagMode(NonEmptyList("serviceTimes", tags("serviceName") :: name :: Nil), Map.empty, true)

      case l@NonEmptyList("error", "instantRate" :: "instantRate" :: Nil) => tagMode(l, Map.empty, true)
      case l@NonEmptyList("error", "instantRateByNode" :: "instantRate" :: Nil) => insertNodeId(l)

      case l@NonEmptyList("error", "instantRate" :: "count" :: Nil) => tagMode(l, Map.empty, true)
      case l@NonEmptyList("error", "instantRateByNode" :: "count" :: Nil) => insertNodeId(l)
        
      //we resort to default mode...
      case _ => tagMode(nameParts, tags, true)
    }
  }

  private def extractTags(nkGlobalParameters: Option[NkGlobalParameters]): Map[String, String] = {
    nkGlobalParameters.map(_.namingParameters) match {
      case Some(Some(params)) => params.tags
      case _ => Map()
    }
  }

}

trait WithMetrics extends RuntimeContextLifecycle {

  @transient protected var metricUtils : MetricUtils = _

  override def open(runtimeContext: RuntimeContext): Unit = {
    this.metricUtils = new MetricUtils(runtimeContext)
  }

}

private class CounterReadProxy(val counter: Counter) extends Counter {
  override def inc(): Unit = {}
  override def inc(n: Long): Unit = {}
  override def dec(): Unit = {}
  override def dec(n: Long): Unit = {}
  override def getCount: Long = counter.getCount
}

private class MeterReadProxy(val meter: Meter) extends Meter {
  override def markEvent(): Unit = {}
  override def markEvent(n: Long): Unit = {}
  override def getRate: Double = meter.getRate
  override def getCount: Long = meter.getCount
}

private class GaugeProxy[T](val gauge: Gauge[T]) extends Gauge[T] {
  override def getValue: T = gauge.getValue
}

private class HistogramReadProxy(val histogram: Histogram) extends Histogram {
  override def update(value: Long): Unit = {}
  override def getCount: Long = histogram.getCount
  override def getStatistics: HistogramStatistics = histogram.getStatistics
}
