from collections import namedtuple

import tensorflow as tf

TargetingDataIdList = 'TargetingDataId'
Target = "Target"

# commented features may be used later, currently not available in bidImp
DEFAULT_CARDINALITIES = {
    "TargetingDataId": 2000003,
    "SupplyVendor": 102,
    # "DealId": 20002,
    "SupplyVendorPublisherId": 200002,
    # "SupplyVendorSiteId": 102,
    "Site": 500002,
    # "AdFormat": 202,
    # "ImpressionPlacementId": 102,
    "Country": 252,
    "Region": 4002,
    # "Metro": 302,
    "City": 150002,
    "Zip": 90002,
    "DeviceMake": 6002,
    "DeviceModel": 40002,
    "RequestLanguages": 5002,
    "RenderingContext": 6,
    #    "UserHourOfWeek": 24,
    #    "AdsTxtSellerType": 7,
    #    "PublisherType": 7,
    "DeviceType": 9,
    "OperatingSystemFamily": 8,
    "Browser": 16,
    # "InternetConnectionType": 10,
    # "MatchedFoldPosition": 5
}

# emb size for each categorical features; later will be specific for each
DEFAULT_EMB_DIM = 6

Feature = namedtuple("Feature", "name, type, cardinality, default_value, embedding_dim")


# "Rule of thumb to pick embedding size corresponding to `n_cat`"
def emb_sz_rule(n_cat):
    return min(DEFAULT_EMB_DIM, round(1.6 * n_cat**0.56))


# preprocessed int and string features
model_features = [
  Feature("SupplyVendor", tf.int32, DEFAULT_CARDINALITIES["SupplyVendor"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["SupplyVendor"])),
  # Feature("DealId", tf.int32, DEFAULT_CARDINALITIES["DealId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DealId"] )),
  Feature("SupplyVendorPublisherId", tf.int32, DEFAULT_CARDINALITIES["SupplyVendorPublisherId"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["SupplyVendorPublisherId"])),
  # Feature("SupplyVendorSiteId", tf.int32, DEFAULT_CARDINALITIES["SupplyVendorSiteId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  Feature("Site", tf.int32, DEFAULT_CARDINALITIES["Site"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Site"])),
  # Feature("ImpressionPlacementId", tf.int32, DEFAULT_CARDINALITIES["ImpressionPlacementId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  Feature("Country", tf.int32, DEFAULT_CARDINALITIES["Country"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Country"])),
  Feature("Region", tf.int32, DEFAULT_CARDINALITIES["Region"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Region"])),
  # Feature("Metro", tf.int32, DEFAULT_CARDINALITIES["Metro"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  Feature("City", tf.int32, DEFAULT_CARDINALITIES["City"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["City"])),
  Feature("Zip", tf.int32, DEFAULT_CARDINALITIES["Zip"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Zip"])),
  Feature("DeviceMake", tf.int32, DEFAULT_CARDINALITIES["DeviceMake"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["DeviceMake"])),
  Feature("DeviceModel", tf.int32, DEFAULT_CARDINALITIES["DeviceModel"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["DeviceModel"])),
  Feature("RequestLanguages", tf.int32, DEFAULT_CARDINALITIES["RequestLanguages"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["RequestLanguages"])),
  Feature("RenderingContext", tf.int32, DEFAULT_CARDINALITIES["RenderingContext"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["RenderingContext"])),
  # Feature("UserHourOfWeek", tf.int32, DEFAULT_CARDINALITIES["UserHourOfWeek"]*7+2, 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  # Feature("AdsTxtSellerType", tf.int32, DEFAULT_CARDINALITIES["AdsTxtSellerType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  # Feature("PublisherType", tf.int32, DEFAULT_CARDINALITIES["PublisherType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] )),
  Feature("DeviceType", tf.int32, DEFAULT_CARDINALITIES["DeviceType"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["DeviceType"])),
  Feature("OperatingSystemFamily", tf.int32, DEFAULT_CARDINALITIES["OperatingSystemFamily"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["OperatingSystemFamily"])),
  Feature("Browser", tf.int32, DEFAULT_CARDINALITIES["Browser"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Browser"]))
] + [
  Feature("AdWidthInPixels", tf.float32, 1, 0.0, None),
  Feature("AdHeightInPixels", tf.float32, 1, 0.0, None),
  Feature("sin_hour_day", tf.float32, 1, 0.0, None),
  Feature("cos_hour_day", tf.float32, 1, 0.0, None),
  Feature("sin_hour_week", tf.float32, 1, 0.0, None),
  Feature("cos_hour_week", tf.float32, 1, 0.0, None),
  Feature("Latitude", tf.float32, 1, 0.0, None),
  Feature("Longitude", tf.float32, 1, 0.0, None)
]

# These are internal types for tracking special tensor objects such as variables and datasets:
model_dim_group = [
    Feature(TargetingDataIdList, tf.variant, DEFAULT_CARDINALITIES[TargetingDataIdList], 0,
            emb_sz_rule(DEFAULT_CARDINALITIES[TargetingDataIdList]))
]

Target_ = namedtuple("Feature", "name, type, default_value")

model_targets = [
    Target_(Target, tf.float32, None)
]
