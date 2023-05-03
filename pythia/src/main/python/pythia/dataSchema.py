# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Functions (with currently hardcoded parameters) to define the default input data schema for model features and target
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
from pythia.dataInput import *
from collections import namedtuple

def defineDataSchemaFeatures():
    Feature = namedtuple("Feature",
                         "name, type, cardinality, default_value, embedding_dim")

    DEFAULT_CARDINALITIES = {
        "RenderingContext" : 6, 
        "DeviceType" : 10, 
        "OperatingSystem": 100, 
        "OperatingSystemFamily" : 10, 
        "Browser": 50, 
        "InternetConnectionType": 10, 
        "PublisherType": 10,  
        "SupplyVendor": 102, 
        "SupplyVendorPublisherId": 200002, 
        "SupplyVendorSiteId": 1000000, 
        "Site": 1000000, 
        "ReferrerUrl": 10000000,   
        "Country": 252, 
        "Region": 4002, 
        "Metro": 302, 
        "City": 150002, 
        "Zip": 90002, 
        "DeviceMake": 6002, 
        "DeviceModel": 40002, 
        "RequestLanguages": 1000, 
        "MatchedLanguageCode": 1000, 
        "UserHourOfWeek": 170
    }    

    # # # # # # # # #
    # MODEL FEATURES
    model_features = [
        Feature("RenderingContext", tf.int32, DEFAULT_CARDINALITIES["RenderingContext"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["RenderingContext"])),
        Feature("DeviceType", tf.int32, DEFAULT_CARDINALITIES["DeviceType"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["DeviceType"])),
        Feature("OperatingSystem", tf.int32, DEFAULT_CARDINALITIES["OperatingSystem"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["OperatingSystem"])),
        Feature("OperatingSystemFamily", tf.int32, DEFAULT_CARDINALITIES["OperatingSystemFamily"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["OperatingSystemFamily"])),
        Feature("Browser", tf.int32, DEFAULT_CARDINALITIES["Browser"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Browser"])),
        Feature("InternetConnectionType", tf.int32, DEFAULT_CARDINALITIES["InternetConnectionType"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["InternetConnectionType"])),
        Feature("PublisherType", tf.int32, DEFAULT_CARDINALITIES["PublisherType"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["PublisherType"])),            
        Feature("SupplyVendor", tf.int32, DEFAULT_CARDINALITIES["SupplyVendor"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["SupplyVendor"])),
        Feature("SupplyVendorPublisherId", tf.int32, DEFAULT_CARDINALITIES["SupplyVendorPublisherId"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["SupplyVendorPublisherId"])),
        Feature("SupplyVendorSiteId", tf.int32, DEFAULT_CARDINALITIES["SupplyVendorSiteId"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["SupplyVendorSiteId"])),
        Feature("Site", tf.int32, DEFAULT_CARDINALITIES["Site"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Site"])),
        #Feature("ReferrerUrl", tf.int32, DEFAULT_CARDINALITIES["ReferrerUrl"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["ReferrerUrl"])), 
        Feature("Country", tf.int32, DEFAULT_CARDINALITIES["Country"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Country"])),
        Feature("Region", tf.int32, DEFAULT_CARDINALITIES["Region"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Region"])),
        Feature("Metro", tf.int32, DEFAULT_CARDINALITIES["Metro"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Metro"])),
        Feature("City", tf.int32, DEFAULT_CARDINALITIES["City"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["City"])),
        Feature("Zip", tf.int32, DEFAULT_CARDINALITIES["Zip"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["Zip"])),
        Feature("DeviceMake", tf.int32, DEFAULT_CARDINALITIES["DeviceMake"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["DeviceMake"])),
        Feature("DeviceModel", tf.int32, DEFAULT_CARDINALITIES["DeviceModel"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["DeviceModel"])),
        Feature("RequestLanguages", tf.int32, DEFAULT_CARDINALITIES["RequestLanguages"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["RequestLanguages"])),
        Feature("MatchedLanguageCode", tf.int32, DEFAULT_CARDINALITIES["MatchedLanguageCode"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["MatchedLanguageCode"])),
        Feature("UserHourOfWeek", tf.int32, DEFAULT_CARDINALITIES["UserHourOfWeek"], 0, emb_sz_rule(DEFAULT_CARDINALITIES["UserHourOfWeek"]))
    ] + [
        Feature("Latitude", tf.float32, 1, 0.0, None),
        Feature("Longitude", tf.float32, 1, 0.0, None),
        Feature("sin_hour_week", tf.float32, 1, 0.0, None),
        Feature("cos_hour_week", tf.float32, 1, 0.0, None),
        Feature("sin_hour_day", tf.float32, 1, 0.0, None),
        Feature("cos_hour_day", tf.float32, 1, 0.0, None),
        Feature("sin_minute_hour", tf.float32, 1, 0.0, None),
        Feature("cos_minute_hour", tf.float32, 1, 0.0, None),
        Feature("sin_minute_day", tf.float32, 1, 0.0, None),
        Feature("cos_minute_day", tf.float32, 1, 0.0, None)
    ]

    return model_features

def defineDataSchemaTargetInterests(target_name, target_length):
    # # # # # # # # #
    # TARGETs FOR THE INTEREST MODEL
    Target_ = namedtuple("Feature", "name, type, default_value, enabled, binary")

    model_targets = [
        Target_(name=target_name, type=tf.int64, default_value=[0 for i in range(target_length)], enabled=True, binary=True)
    ] 
  
    return model_targets