package com.ttd.features.visualise

import com.ttd.features.transformers.{CreateGlobalTempTable, Join, ReadGlobalTempTable}
import org.apache.spark.ml.param.ParamPair
import org.apache.spark.ml._

class PipelineAnalyser(maxDepth: Int = 10) {
  var globalCurrNode: String = "start"
  var tempTables: Map[String, String] = Map.empty[String, String]
  var currDepth = 0

  def explore(root: PipelineStage): String = {
    val sb = new StringBuilder()
    sb ++= "digraph G {"

    val (nodeIn, nodeOut) = _explorePipelineStage(root, sb)
    if (nodeIn != null && nodeOut != null) {
      sb ++= "start -> " + nodeIn + ";"
      sb ++= nodeOut + " -> end;"
    } else {
      sb ++= "start -> end;"
    }

    sb ++= "}"

    sb.toString
  }

  def _explorePipeline(pipeline: Pipeline, sb:StringBuilder): (String, String) = {
    currDepth += 1

    sb ++= "subgraph cluster"+pipeline.uid+" {"
    sb ++= "node [shape=record, color=green];"
    sb ++= "color=lightgrey;"
    sb ++= "label=\""+pipeline.getClass.getSimpleName+"\";"

    val innerSb = if (currDepth >= maxDepth) {
      new StringBuilder()
    } else {
      sb
    }

    var isFirst = true
    var nodeIn:String = null
    var nodeOut:String = null
    var curNode:String = null
    pipeline.getStages.foreach{stage => {
      val (nIn, nOut) = _explorePipelineStage(stage, innerSb)
      if (isFirst) {
        nodeIn = nIn
      }
      isFirst = false
      if (nIn == null) {
        // ReadGlobalTempNode
        curNode = nOut
        nodeOut = nOut
      } else {
        nodeOut = nOut

        if (curNode != null) {
          innerSb ++= curNode + " -> " + nIn + ";"
        }
        curNode = nOut
      }}}

    sb ++= "};"

    currDepth -= 1
    (nodeIn, nodeOut)
  }

  def _explorePipelineModel(model: PipelineModel, sb:StringBuilder): (String, String) = {
    currDepth += 1

    sb ++= "subgraph cluster"+model.uid+" {"
    sb ++= "node [shape=record, color=red];"
    sb ++= "color=darkgrey;"
    sb ++= "label=\""+model.uid+":"+model.getClass.getSimpleName+"\";"

    val innerSb = if (currDepth >= maxDepth) {
      new StringBuilder()
    } else {
      sb
    }

    var isFirst = true
    var nodeIn:String = null
    var nodeOut:String = null
    var curNode:String = null
    model.stages.foreach{stage => {
      val (nIn, nOut) = _explorePipelineStage(stage, innerSb)
      if (isFirst) {
        nodeIn = nIn
      }
      isFirst = false
      if (nIn == null) {
        // ReadGlobalTempNode
        curNode = nOut
        nodeOut = nOut
      } else {
        nodeOut = nOut

        if (curNode != null) {
          innerSb ++= curNode + " -> " + nIn + ";"
        }
        curNode = nOut
    }}}

    sb ++= "};"

    currDepth -= 1
    (nodeIn, nodeOut)
  }

  def _paramString(p: ParamPair[_]): String = {
    val valueString = p.value match {
      case array: Array[_] => array.mkString(",")
      case x => x.toString
    }

    p.param.name + ":" + p.value.getClass.getSimpleName + " = " +
      valueString.map(c => if ((c == '>') || (c == '<')) "\\"+c else c).mkString("")
  }

  def _summariseParams(params:Seq[ParamPair[_]]):String = {
    val content: Seq[Any] = params
      .map{_paramString}
      .mkString("|")
    "{params|{" + content + "}}"
  }

  def _exploreTransformer(trans:Transformer, sb:StringBuilder): String = {
    val name = try {
      "{Transformer|"+ trans.getClass.getSimpleName+"}"
    } catch {
      case _: Throwable => "{Transformer|"+ trans.uid +"}"
    }

    val paramsContent = _summariseParams(trans.extractParamMap.toSeq)
    sb ++= trans.uid + " [label=\"{"+name+"|"+paramsContent+"}\"];"

    trans.uid
  }

  def _exploreEstimator(estim:Estimator[_], sb:StringBuilder): String = {
    val name = try {
      "{Estimator|"+ estim.getClass.getSimpleName+"}"
    } catch {
      case _: Throwable => "{Estimator|"+ estim.uid +"}"
    }
    val paramsContent = _summariseParams(estim.extractParamMap.toSeq)
    sb ++= estim.uid + " [label=\"{"+name+"|"+paramsContent+"}\"];"

    estim.uid
  }

  def _exploreJoin(join:Join, sb:StringBuilder): String = {
    val name = "{Join |"+ join.getClass.getSimpleName+"}"
    val paramsContent = _summariseParams(join.extractParamMap.toSeq)
    sb ++= join.uid + " [label=\"{"+name+"|"+paramsContent+"}\"];"

    val rightTempName = join.get(join.rightTempName).get
    if (tempTables.contains(rightTempName)) {
      // exists as output of earlier part of the pipeline
      sb ++= tempTables(rightTempName) + " -> " + join.uid + ";"
    } else {
      // expected input from placeholder
      sb ++= rightTempName + " [label=\"{"+"Placeholder for: "+rightTempName+"}\"];"
      sb ++= rightTempName + " -> " + join.uid + ";"
    }
    join.uid
  }

  // todo: optionally hide this node
  def _exploreCreateGlobalTempTable(createGlobalTempTable: CreateGlobalTempTable, sb:StringBuilder): String = {
    val name = "{CreateGlobalTempTable |"+ createGlobalTempTable.getClass.getSimpleName+"}"
    val paramsContent = _summariseParams(createGlobalTempTable.extractParamMap.toSeq)
    sb ++= createGlobalTempTable.uid + " [label=\"{"+name+"|"+paramsContent+"}\"];"

    val tempName = createGlobalTempTable.get(createGlobalTempTable.tempTableName).get
    tempTables += (tempName -> createGlobalTempTable.uid)
    createGlobalTempTable.uid
  }

  // todo: shouldn't have a predecessor! read is its own input or previous input
  def _exploreReadGlobalTempTable(readGlobalTempTable: ReadGlobalTempTable, sb:StringBuilder): String = {
    val name = "{ReadGlobalTempTable |"+ readGlobalTempTable.getClass.getSimpleName+"}"
    val paramsContent = _summariseParams(readGlobalTempTable.extractParamMap.toSeq)
    sb ++= readGlobalTempTable.uid + " [label=\"{"+name+"|"+paramsContent+"}\"];"

    val tempName = readGlobalTempTable.get(readGlobalTempTable.tempTableName).get
    // todo: put in tempTables
//    tempTables += (tempName -> readGlobalTempTable.uid)
    if (tempTables.contains(tempName)) {
      // exists as output of earlier part of the pipeline
      sb ++= tempTables(tempName) + " -> " + readGlobalTempTable.uid + ";"
    }
    readGlobalTempTable.uid
  }

  def _explorePipelineStage(stage: PipelineStage, sb:StringBuilder): (String, String) = {
    stage match {
      case pipeline: Pipeline =>
        _explorePipeline(pipeline, sb)
      case pipelineModel: PipelineModel =>
        _explorePipelineModel(pipelineModel, sb)
      case join: Join =>
        val node = _exploreJoin(join, sb)
        (node, node)
      case createGlobalTempTable: CreateGlobalTempTable =>
        val node = _exploreCreateGlobalTempTable(createGlobalTempTable, sb)
        (node, node)
      case readGlobalTempTable: ReadGlobalTempTable =>
        val node = _exploreReadGlobalTempTable(readGlobalTempTable, sb)
        (null, node)
      case transformer: Transformer =>
        val node = _exploreTransformer(transformer, sb)
        (node, node)
      case estim: Estimator[_] =>
        val node = _exploreEstimator(estim, sb)
        (node, node)
      case _ =>
        (null, null)
    }
  }
}