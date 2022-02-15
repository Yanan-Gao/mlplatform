package com.ttd.features.transformers.util

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types.StructType

/**
 * Help object for merging schemas when joining
 */
object JoinSchema {
  def mergeSchemas(a: StructType, b: StructType): StructType = {
    val aFields: Set[String] = a.fields.map(_.name).toSet
    val fields = a.fields ++ b.fields.filter(f => !aFields.contains(f.name))
    new StructType(fields)
  }

  @scala.annotation.tailrec
  def mergeSchemaOnJoinType(jt: JoinType, a: StructType, b: StructType): StructType = {
    jt match {
      case like: InnerLike =>
        mergeSchemas(a, b)
      case LeftOuter =>
        mergeSchemas(a, b)
      case RightOuter =>
        mergeSchemas(a, b)
      case FullOuter =>
        mergeSchemas(a, b)
      case LeftSemi =>
        a
      case LeftAnti =>
        a
      case ExistenceJoin(_) =>
        throw new UnsupportedOperationException
      case NaturalJoin(tpe) =>
        mergeSchemas(a, b)
      case UsingJoin(tpe, usingColumns) =>
        mergeSchemaOnJoinType(tpe, a, b)
    }
  }
}
