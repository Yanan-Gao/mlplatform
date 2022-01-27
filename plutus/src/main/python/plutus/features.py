from collections import namedtuple

import tensorflow as tf

Feature = namedtuple("Feature", "name, sparse, type, cardinality, default_value, enabled, embedding_dim, qr_embed, qr_collisions")

DEFAULT_EMB_DIM = 16
DEFAULT_QR_COLLISION = 4
DEFAULT_CARDINALITIES = {
    "SupplyVendor": 102,
    "DealId": 5002,
    "SupplyVendorPublisherId": 15002,
    "SupplyVendorSiteId": 102,
    "Site": 350002,
    "AdFormat": 102,
    "ImpressionPlacementId": 102,
    "Country": 252,
    "Region": 4002,
    "Metro": 302,
    "City": 75002,
    "Zip": 90002,
    "DeviceMake": 1002,
    "DeviceModel": 10002,
    "RequestLanguages": 502,
    "RenderingContext": 6,
    "UserHourOfWeek": 24,
    "AdsTxtSellerType": 7,
    "PublisherType": 7,
    "DeviceType": 9,
    "OperatingSystemFamily": 10,
    "Browser": 20
}


default_model_features = [
                             Feature("SupplyVendor", True, tf.int32, DEFAULT_CARDINALITIES["SupplyVendor"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("DealId", True, tf.int32, DEFAULT_CARDINALITIES["DealId"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("SupplyVendorPublisherId", True, tf.int32, DEFAULT_CARDINALITIES["SupplyVendorPublisherId"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("SupplyVendorSiteId", True, tf.int32, DEFAULT_CARDINALITIES["SupplyVendorSiteId"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("Site", True, tf.int32, DEFAULT_CARDINALITIES["Site"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("AdFormat", True, tf.int32, DEFAULT_CARDINALITIES["AdFormat"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("ImpressionPlacementId", True, tf.int32, DEFAULT_CARDINALITIES["ImpressionPlacementId"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("Country", True, tf.int32, DEFAULT_CARDINALITIES["Country"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("Region", True, tf.int32, DEFAULT_CARDINALITIES["Region"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("Metro", True, tf.int32, DEFAULT_CARDINALITIES["Metro"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("City", True, tf.int32, DEFAULT_CARDINALITIES["City"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("Zip", True, tf.int32, DEFAULT_CARDINALITIES["Zip"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("DeviceMake", True, tf.int32, DEFAULT_CARDINALITIES["DeviceMake"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("DeviceModel", True, tf.int32, DEFAULT_CARDINALITIES["DeviceModel"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("RequestLanguages", True, tf.int32, DEFAULT_CARDINALITIES["RequestLanguages"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("RenderingContext", True, tf.int32, DEFAULT_CARDINALITIES["RenderingContext"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("UserHourOfWeek", True, tf.int32, DEFAULT_CARDINALITIES["UserHourOfWeek"]*7+2, 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("AdsTxtSellerType", True, tf.int32, DEFAULT_CARDINALITIES["AdsTxtSellerType"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("PublisherType", True, tf.int32, DEFAULT_CARDINALITIES["PublisherType"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("DeviceType", True, tf.int32, DEFAULT_CARDINALITIES["DeviceType"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("OperatingSystemFamily", True, tf.int32, DEFAULT_CARDINALITIES["OperatingSystemFamily"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION),
                             Feature("Browser", True, tf.int32, DEFAULT_CARDINALITIES["Browser"], 0, True, DEFAULT_EMB_DIM, False, DEFAULT_QR_COLLISION)
                         ] + [
                             Feature("sin_hour_day", False, tf.float32, 1, 0.0, True, None, False, None),
                             Feature("cos_hour_day", False, tf.float32, 1, 0.0, True, None, False, None),
                             Feature("sin_minute_hour", False, tf.float32, 1, 0.0, True, None, False, None),
                             Feature("cos_minute_hour", False, tf.float32, 1, 0.0, True, None, False, None),
                             Feature("sin_hour_week", False, tf.float32, 1, 0.0, True, None, False, None),
                             Feature("cos_hour_week", False, tf.float32, 1, 0.0, True, None, False, None),
                             Feature("latitude", False, tf.float32, 1, 0.0, True, None, False, None),
                             Feature("longitude", False, tf.float32, 1, 0.0, True, None, False, None)
                         ]


Target = namedtuple("Feature", "name, type, default_value, enabled, binary")

default_model_targets = [
    Target("is_imp", tf.float32, None, True, True),
    Target("AuctionBidPrice", tf.float32, None, True, False),
    Target("RealMediaCost", tf.float32, 0.0, True, False),
    Target("mb2w", tf.float32, None, True, False),
    Target("FloorPriceInUSD", tf.float32, 0.0, True, False)
]


def get_model_features(exclude_model_features=None):
    if exclude_model_features is None:
        exclude_model_features = []
    return [feat for feat in default_model_features if feat.name not in exclude_model_features]


def get_model_targets(exclude_model_targets=None):
    if exclude_model_targets is None:
        exclude_model_targets = []
    return [target for target in default_model_targets if target.name not in exclude_model_targets]


def get_int_parser(model_features, model_targets):
    feature_description_ints = {f.name: tf.io.FixedLenFeature([], tf.int64 if f.type == tf.int32 else tf.float32, f.default_value) for f in model_features }
    feature_description_ints.update({t.name: tf.io.FixedLenFeature([], t.type, t.default_value) for t in model_targets})

    def parse_just_ints(example):
        data = tf.io.parse_example(example, feature_description_ints)

        targets = tf.stack(
            [
                data.pop('is_imp'),
                data.pop('AuctionBidPrice'),
                data.pop('RealMediaCost'),
                data.pop('mb2w'),
                data.pop('FloorPriceInUSD')
            ], axis=-1)
        return data, targets

    return parse_just_ints
