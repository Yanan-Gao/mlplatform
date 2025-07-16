package com.thetradedesk.featurestore.aggfunctions

object AggFunctions {
  sealed trait AggFuncV2

  object AggFuncV2 {
    case object Sum extends AggFuncV2

    case object Count extends AggFuncV2

    case object Mean extends AggFuncV2

    case object NonZeroMean extends AggFuncV2

    case object Median extends AggFuncV2

    case object Desc extends AggFuncV2

    case object NonZeroCount extends AggFuncV2

    case object Min extends AggFuncV2

    case object Max extends AggFuncV2

    case class Frequency(topNs: Array[Int] = Array(-1)) extends AggFuncV2 {
      override def toString: String = {
        "Frequency"
      }

      override def equals(obj: Any): Boolean = {
        obj match {
          case that: Frequency => this.topNs.sameElements(that.topNs)
          case _ => false
        }
      }

      override def hashCode(): Int = topNs.hashCode()
    }

    case class Percentile(percentiles: Array[Int]) extends AggFuncV2 {
      override def equals(obj: Any): Boolean = {
        obj match {
          case that: Percentile => this.percentiles.sameElements(that.percentiles)
          case _ => false
        }
      }

      override def hashCode(): Int = percentiles.hashCode()
    }
    case object QuantileSummary extends AggFuncV2

    def parseAggFuncs(funcStrings: Seq[String], field: String): Seq[AggFuncV2] = {
      val funcs = funcStrings.map { x =>
        fromString(x).getOrElse(
          throw new IllegalArgumentException(s"Unknown aggregation function: $x for field: $field")
        )
      }
      compactAggFuncs(funcs)
    }

    def compactAggFuncs(funcs: Seq[AggFuncV2]): Seq[AggFuncV2] = {
      val (frequencies, rest1) = funcs.partition {
        case Frequency(_) => true
        case _ => false
      }
      val (percentiles, rest) = rest1.partition {
        case Percentile(_) => true
        case _ => false
      }

      val mergedFrequency = if (frequencies.nonEmpty) {
        // Merge all topNs arrays into one, remove duplicates, keep -1 if present
        val allTopNs = frequencies.collect { case Frequency(topNs) => topNs }.flatten
        val mergedTopNs = allTopNs.distinct.sorted.toArray
        Seq(Frequency(mergedTopNs))
      } else Seq.empty

      val mergedPercentile = if (percentiles.nonEmpty) {
        val allPercentiles = percentiles.collect { case Percentile(arr) => arr }.flatten
        val mergedPercentiles = allPercentiles.distinct.sorted.toArray
        Seq(Percentile(mergedPercentiles))
      } else Seq.empty

      rest.distinct ++ mergedFrequency ++ mergedPercentile
    }

    def getExportFuncs(funcs: Seq[AggFuncV2]): Seq[AggFuncV2] = {
      val (desc, rest) = funcs.partition {
        case Desc => true
        case _ => false
      }
      if (desc.isEmpty) {
        compactAggFuncs(rest)
      } else {
        val descFuncs = Array(Count, Sum, Min, Max, Mean, NonZeroMean, Percentile(Array(10, 25, 50, 75, 90)))
        compactAggFuncs(rest ++ descFuncs)
      }
    }

    def getMergeableFuncs(funcs: Seq[AggFuncV2]): Seq[AggFuncV2] = {
      val expandedFuncs = funcs.flatMap {
        case Mean => Seq(Count, Sum)
        case NonZeroMean => Seq(NonZeroCount, Sum)
        case Desc => Seq(Count, Sum, Min, Max, NonZeroCount, QuantileSummary)
        case Percentile(_) => Seq(QuantileSummary)
        case other => Seq(other)
      }
      val compacted = compactAggFuncs(expandedFuncs)
      // special case for frequency
      val (frequencies, rest) = compacted.partition {
        case Frequency(_) => true
        case _ => false
      }
      if (frequencies.nonEmpty) {
        val topNs = frequencies.head.asInstanceOf[Frequency].topNs
        val topN = if (topNs.contains(-1)) -1 else topNs.max * 5
        Seq(Frequency(Array(topN))) ++ rest
      } else {
        compacted
      }
    }


    def fromString(str: String): Option[AggFuncV2] = {
      str.trim.toLowerCase match {
        case s if s.equals("sum") => Some(Sum)
        case s if s.equals("count") => Some(Count)
        case s if s.equals("mean") => Some(Mean)
        case s if s.equals("nonzeromean") => Some(NonZeroMean)
        case s if s.equals("median") => Some(Median)
        case s if s.equals("desc") => Some(Desc)
        case s if s.equals("nonzerocount") => Some(NonZeroCount)
        case s if s.equals("min") => Some(Min)
        case s if s.equals("max") => Some(Max)
        case s if s.equals("frequency") => Some(Frequency())
        case s if s.startsWith("top") =>
          // Try to match TopN(n)
          val topNPattern = """(?i)top\s*(\d+)""".r
          s match {
            case topNPattern(n) => Some(Frequency(Array(n.toInt)))
            case _ => None
          }
        case s if s.startsWith("p") =>
          // Try to match P(n)
          val percentilePattern = """(?i)p\s*(\d+)""".r
          s match {
            case percentilePattern(n) => Some(Percentile(Array(n.toInt)))
            case _ => None
          }
        case _ => None
      }
    }
  }
}
