package com.ttd.contextual.util.elDoradoUtilities.datasets.core

import com.ttd.contextual.util.elDoradoUtilities.spark.TTDConfig.config

abstract class QuboleVerticaS3DataSet[T <: Product](rootFolderPath: String, mergeSchema: Boolean = false)(implicit m: Manifest[T])
    extends DatePartitionedS3DataSet[T](GeneratedDataSet, S3Roots.QUBOLE_VERTICA_ROOT, rootFolderPath, mergeSchema = mergeSchema) {
        override protected val s3RootProd: String = normalizeUri(config.getString(s"ttd.$dataSetName.prod.s3root", s"$s3Root"))
        override protected val s3RootTest: String = normalizeUri(config.getString(s"ttd.$dataSetName.test.s3root", s"$s3Root"))
}
