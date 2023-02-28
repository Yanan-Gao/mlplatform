import tensorflow as tf
import numpy as np

DEFAULT_EMB_DIM = 16

DEFAULT_CARDINALITIES = {
    "AdFormat": 202,
    "AdGroupId": 500002, # this is a value we will most likely not use
    # "AdsTxtSellerType": 7,
    "Browser": 15,
    "City": 150002,
    "Country": 252,
    # "DealId": 20002,
    "DeviceMake": 6002,
    "DeviceModel": 40002,
    "DeviceType": 9,
    # "ImpressionPlacementId": 102,
    "InternetConnectionType": 10,
    "MatchedFoldPosition": 5,
    # "Metro": 302,
    "OperatingSystem": 72,
    # "PublisherType": 7,
    "Region": 4002,
    "RenderingContext": 6,
    "RequestLanguages": 5002,
    "Site": 500002,
    "SupplyVendor": 102,
    "SupplyVendorPublisherId": 200002,
    # "SupplyVendorSiteId": 102,
    # "UserHourOfWeek": 24,
    "Zip": 90002,
    "HasContextualCategory": 3,
    "ContextualCategories": 700,
    "HasContextualCategoryTier1": 3,
    "ContextualCategoriesTier1": 31,
}

# ppmethod stands for pre-processing method: simple, int_vocab, string_vocab, string_map
class Feature:
    def __init__(self, name, type, cardinality, default_value, embedding_dim=None, ppmethod=None, column_name=None, preprocessor=None, base_type=None):
        self.name = name
        self.type = type
        self.cardinality = cardinality
        self.default_value = default_value
        self.embedding_dim = embedding_dim
        self.ppmethod = ppmethod
        self.column_name = column_name or self.name # for list feature
        self.preprocessor = preprocessor
        self.base_type = base_type or type

    def __repr__(self):
        return "name=%s type=%s cardinality=%s default_value=%s embedding_dim=%s ppmethod=%s column_name=%s base_type=%s" % (
            self.name,
            self.type,
            self.cardinality,
            self.default_value,
            self.embedding_dim,
            self.ppmethod,
            self.column_name,
            self.base_type
        )

def emb_sz_rule(n_cat):
    "Rule of thumb to pick embedding size corresponding to `n_cat`"
    return min(DEFAULT_EMB_DIM, round(1.6 * n_cat**0.56))


contextual_tier1_features = [
    Feature("ContextualCategoryLengthTier1", tf.float32, 1, 0.0),
    # has tier1 contextual or not
    # default value is 1
    Feature("HasContextualCategoryTier1", tf.int64,
            DEFAULT_CARDINALITIES["HasContextualCategoryTier1"], 1,
            emb_sz_rule(DEFAULT_CARDINALITIES["HasContextualCategoryTier1"]), "simple"),
    # tier1 contextual categories
    # default value is all zeros
    Feature("ContextualCategoriesTier1", tf.variant,
            DEFAULT_CARDINALITIES["ContextualCategoriesTier1"], [0]*DEFAULT_CARDINALITIES["ContextualCategoriesTier1"],
            emb_sz_rule(DEFAULT_CARDINALITIES["ContextualCategoriesTier1"]), "simple",
            ["ContextualCategoriesTier1_Column{}".format(i) for i in range(DEFAULT_CARDINALITIES["ContextualCategoriesTier1"])],
            None, tf.int64),
]

extended_features = {
    "contextual": contextual_tier1_features,
}

#preprocessed int and string features
default_model_features = [ # 17 active features
    Feature("AdFormat", tf.int64, DEFAULT_CARDINALITIES["AdFormat"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["AdFormat"] ), "simple"),
    # Feature("AdsTxtSellerType", tf.int64, DEFAULT_CARDINALITIES["AdsTxtSellerType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("Browser", tf.int64, DEFAULT_CARDINALITIES["Browser"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Browser"] ), "simple"),
    Feature("City", tf.int64, DEFAULT_CARDINALITIES["City"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["City"] ), "simple"),
    Feature("Country", tf.int64, DEFAULT_CARDINALITIES["Country"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Country"] ), "simple"),
    # Feature("DealId", tf.int64, DEFAULT_CARDINALITIES["DealId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DealId"] ), "simple"),
    Feature("DeviceMake", tf.int64, DEFAULT_CARDINALITIES["DeviceMake"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DeviceMake"] ), "simple"),
    Feature("DeviceModel", tf.int64, DEFAULT_CARDINALITIES["DeviceModel"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DeviceModel"] ), "simple"),
    Feature("DeviceType", tf.int64, DEFAULT_CARDINALITIES["DeviceType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["DeviceType"] ), "simple"),
    # Feature("ImpressionPlacementId", tf.int64, DEFAULT_CARDINALITIES["ImpressionPlacementId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("InternetConnectionType", tf.int64, DEFAULT_CARDINALITIES["InternetConnectionType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["InternetConnectionType"] ), "simple"),
    Feature("MatchedFoldPosition", tf.int64, DEFAULT_CARDINALITIES["MatchedFoldPosition"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["MatchedFoldPosition"] ), "simple"),
    # Feature("Metro", tf.int64, DEFAULT_CARDINALITIES["Metro"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("OperatingSystem", tf.int64, DEFAULT_CARDINALITIES["OperatingSystem"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["OperatingSystem"] ), "simple"),
    # Feature("PublisherType", tf.int64, DEFAULT_CARDINALITIES["PublisherType"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("Region", tf.int64, DEFAULT_CARDINALITIES["Region"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Region"] ), "simple"),
    Feature("RenderingContext", tf.int64, DEFAULT_CARDINALITIES["RenderingContext"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["RenderingContext"] ), "simple"),
    Feature("RequestLanguages", tf.int64, DEFAULT_CARDINALITIES["RequestLanguages"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["RequestLanguages"] ), "simple"),
    Feature("Site", tf.int64, DEFAULT_CARDINALITIES["Site"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Site"] ), "simple"),
    Feature("SupplyVendor", tf.int64, DEFAULT_CARDINALITIES["SupplyVendor"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("SupplyVendorPublisherId", tf.int64, DEFAULT_CARDINALITIES["SupplyVendorPublisherId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendorPublisherId"] ), "simple"),
    # Feature("SupplyVendorSiteId", tf.int64, DEFAULT_CARDINALITIES["SupplyVendorSiteId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    # Feature("UserHourOfWeek", tf.int64, DEFAULT_CARDINALITIES["UserHourOfWeek"]*7+2, 0, emb_sz_rule( DEFAULT_CARDINALITIES["SupplyVendor"] ), "simple"),
    Feature("Zip", tf.int64, DEFAULT_CARDINALITIES["Zip"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["Zip"] ), "simple"),
] + [ # 8 features
    Feature("sin_hour_day", tf.float32, 1, 0.0),
    Feature("cos_hour_day", tf.float32, 1, 0.0),
    Feature("sin_minute_hour", tf.float32, 1, 0.0),
    Feature("cos_minute_hour", tf.float32, 1, 0.0),
    Feature("sin_hour_week", tf.float32, 1, 0.0),
    Feature("cos_hour_week", tf.float32, 1, 0.0),
    Feature("latitude", tf.float32, 1, 0.0),
    Feature("longitude", tf.float32, 1, 0.0)
]

default_model_dim_group = Feature("AdGroupId", tf.int64, DEFAULT_CARDINALITIES["AdGroupId"], 0, emb_sz_rule( DEFAULT_CARDINALITIES["AdGroupId"] ), "simple")


default_model_targets = [Feature("Target", tf.int64, cardinality=None, default_value=None)]

def get_target_cat_map(model_targets, card_cap):
    #get categorical target dict for AE
    cat_dict={}
    cat_dict.update({f.name: f.cardinality for f in model_targets if f.type == tf.string})
    cat_dict.update({f.name: card_cap for f in model_targets if f.type == tf.int64 and f.cardinality == card_cap})
    cat_dict.update({f.name: f.cardinality for f in model_targets if f.type == tf.int64 and f.cardinality != card_cap})
    return cat_dict