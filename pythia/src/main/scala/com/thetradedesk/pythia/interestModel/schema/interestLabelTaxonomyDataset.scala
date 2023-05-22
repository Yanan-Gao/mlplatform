package com.thetradedesk.pythia.interestModel.schema

case class interestLabelTaxonomyRecord(
  SelectedChina: Int, 
  Selected: Int,	
  IABAudienceV11_UniqueId: Int,
  IABAudienceV11_ParentId: Int,
  IABAudienceV11_TopLevelId: Int,
  IABAudienceV11_Depth: Int,	
  IABAudienceV11_FullPath: String,	
  TTDContextual_CategoryId: Int,
  TTDContextual_UniversalCategoryTaxonomyId: String,	
  TTDContextual_ttdStId: Int,	
  TTDContextual_Depth: Int,	
  TTDContextual_ContextualStandardCategoryName: String,	
  TTD_classificationTaxonomyId: Int,	
  TTD_parentId: Int,	
  TTD_depth: Int,	
  TTD_categoryLabel: Int,	
  TTD_countryBlacklist: String,	
  TTD_generateOnlyCertainSegmentLevels: String,	
  TargetingDataId_100: Int,	
  TargetingDataId_50: Int,	
  TargetingDataId_25: Int,	
  TargetingDataId_10: Int,	
  TargetingDataId_5: Int,	
  TargetingDataId_1: Int
)

object interestLabelTaxonomyDataset {
  val TAXS3 : String = "s3://thetradedesk-mlplatform-us-east-1/data/prod/pythia/interests/taxMap_withTgtIds.txt"
}