from collections import namedtuple
import tensorflow as tf

DEFAULT_EMB_DIM = 16

DEFAULT_CARDINALITIES = {
    "SupplyVendor": 102,
    "DealId": 20002,
    "SupplyVendorPublisherId": 200002,
    #"SupplyVendorSiteId": 102,
    "Site": 500002,
    "AdFormat": 202,
    #"ImpressionPlacementId": 102,
    "Country": 252,
    "Region": 4002,
    #"Metro": 302,
    "City": 150002,
    "Zip": 90002,
    "DeviceMake": 6002,
    "DeviceModel": 40002,
    "RequestLanguages": 5002,
    "RenderingContext": 6,
#    "UserHourOfWeek": 24,
#    "AdsTxtSellerType": 7,
#    "PublisherType": 7,
    "AdGroupId": 500002, # this is a value we will most likely not use
    "DeviceType": 9,
    "OperatingSystem": 72,
    "Browser": 15,
    "InternetConnectionType": 10,
    "MatchedFoldPosition": 5
}

Feature = namedtuple("Feature", "name, type, cardinality, default_value, embedding_dim, ppmethod") #ppmethod stands for pre-processing method: simple, int_vocab, string_vocab, string_map

def emb_sz_rule(n_cat):
    "Rule of thumb to pick embedding size corresponding to `n_cat`"
    return min(DEFAULT_EMB_DIM, round(1.6 * n_cat**0.56))

#preprocessed int and string features
default_model_features = [
    Feature("SupplyVendor", tf.int64, DEFAULT_CARDINALITIES["SupplyVendor"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    #    Feature("DealId", tf.int64, DEFAULT_CARDINALITIES["DealId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DealId"] ), "simple"),
    Feature("SupplyVendorPublisherId", tf.int64, DEFAULT_CARDINALITIES["SupplyVendorPublisherId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendorPublisherId"] ), "simple"),
    #    Feature("SupplyVendorSiteId", tf.int64, DEFAULT_CARDINALITIES["SupplyVendorSiteId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("Site", tf.int64, DEFAULT_CARDINALITIES["Site"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Site"] ), "simple"),
    Feature("AdFormat", tf.int64, DEFAULT_CARDINALITIES["AdFormat"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["AdFormat"] ), "simple"),
    #    Feature("ImpressionPlacementId", tf.int64, DEFAULT_CARDINALITIES["ImpressionPlacementId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("Country", tf.int64, DEFAULT_CARDINALITIES["Country"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Country"] ), "simple"),
    Feature("Region", tf.int64, DEFAULT_CARDINALITIES["Region"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Region"] ), "simple"),
    #    Feature("Metro", tf.int64, DEFAULT_CARDINALITIES["Metro"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("City", tf.int64, DEFAULT_CARDINALITIES["City"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["City"] ), "simple"),
    Feature("Zip", tf.int64, DEFAULT_CARDINALITIES["Zip"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Zip"] ), "simple"),
    Feature("DeviceMake", tf.int64, DEFAULT_CARDINALITIES["DeviceMake"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DeviceMake"] ), "simple"),
    Feature("DeviceModel", tf.int64, DEFAULT_CARDINALITIES["DeviceModel"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DeviceModel"] ), "simple"),
    Feature("RequestLanguages", tf.int64, DEFAULT_CARDINALITIES["RequestLanguages"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["RequestLanguages"] ), "simple"),
    Feature("RenderingContext", tf.int64, DEFAULT_CARDINALITIES["RenderingContext"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["RenderingContext"] ), "simple"),
    #    Feature("UserHourOfWeek", tf.int64, DEFAULT_CARDINALITIES["UserHourOfWeek"]*7+2, 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    #    Feature("AdsTxtSellerType", tf.int64, DEFAULT_CARDINALITIES["AdsTxtSellerType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    #    Feature("PublisherType", tf.int64, DEFAULT_CARDINALITIES["PublisherType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("DeviceType", tf.int64, DEFAULT_CARDINALITIES["DeviceType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DeviceType"] ), "simple"),
    Feature("OperatingSystem", tf.int64, DEFAULT_CARDINALITIES["OperatingSystem"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["OperatingSystem"] ), "simple"),
    Feature("Browser", tf.int64, DEFAULT_CARDINALITIES["Browser"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Browser"] ), "simple"),
    Feature("InternetConnectionType", tf.int64, DEFAULT_CARDINALITIES["InternetConnectionType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["InternetConnectionType"] ), "simple"),
    Feature("MatchedFoldPosition", tf.int64, DEFAULT_CARDINALITIES["MatchedFoldPosition"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["MatchedFoldPosition"] ), "simple")
] + [
    Feature("sin_hour_day", tf.float32, 1, 0.0, None, None),
    Feature("cos_hour_day", tf.float32, 1, 0.0, None, None),
    Feature("sin_minute_hour", tf.float32, 1, 0.0, None, None),
    Feature("cos_minute_hour", tf.float32, 1, 0.0, None, None),
    Feature("sin_hour_week", tf.float32, 1, 0.0, None, None),
    Feature("cos_hour_week", tf.float32, 1, 0.0, None, None),
    Feature("latitude", tf.float32, 1, 0.0, None, None),
    Feature("longitude", tf.float32, 1, 0.0, None, None)
]

#
default_model_dim_group = Feature("AdGroupId", tf.int64, DEFAULT_CARDINALITIES["AdGroupId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["AdGroupId"] ), "simple")


Target = namedtuple("Feature", "name, type, default_value")

default_model_targets = [Target("Target", tf.int64, None)]

def get_target_cat_map(model_targets, card_cap):
    #get categorical target dict for AE
    cat_dict={}
    cat_dict.update({f.name: f.cardinality for f in model_targets if f.type == tf.string})
    cat_dict.update({f.name: f.cardinality for f in model_targets if f.type == tf.int64 and f.cardinality == card_cap})
    cat_dict.update({f.name: f.cardinality for f in model_targets if f.type == tf.int64 and f.cardinality != card_cap})
    return cat_dict