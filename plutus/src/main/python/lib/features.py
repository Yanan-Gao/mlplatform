from collections import namedtuple
import tensorflow as tf

Feature = namedtuple("Feature",
                     "name, type, cardinality, default_value, enabled, embedding_dim, qr_embed, qr_collisions")

DEFAULT_EMB_DIM = 16


model_features = [
    Feature("SupplyVendor", tf.int64, 102, 0, True, DEFAULT_EMB_DIM, False, 4),
    # Feature("DealId", tf.int64, 5002, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("SupplyVendorPublisherId", tf.int64, 15002, 0, True, DEFAULT_EMB_DIM, True, 4),
    Feature("SupplyVendorSiteId", tf.int64, 102, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("Site", tf.int64, 350002, 0, True, DEFAULT_EMB_DIM, True, 4),
    Feature("AdFormat", tf.int64, 102, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("MatchedCategory", tf.int64, 4002, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("ImpressionPlacementId", tf.int64, 102, 0, True, DEFAULT_EMB_DIM, False, 4),
    #     Feature("Carrier", tf.int64, 202, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("Country", tf.int64, 2364, 0, True, DEFAULT_EMB_DIM, False, 4),
    #       Feature("Region", tf.int64, 4002, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("Metro", tf.int64, 302, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("City", tf.int64, 75002, 0, True, DEFAULT_EMB_DIM, True, 4),
    Feature("Zip", tf.int64, 90002, 0, True, DEFAULT_EMB_DIM, True, 4),
    Feature("DeviceMake", tf.int64, 2927, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("DeviceModel", tf.int64, 10002, 0, True, DEFAULT_EMB_DIM, True, 4),
    Feature("RequestLanguages", tf.int64, 502, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("RenderingContext", tf.int64, 6, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("MatchedFoldPosition", tf.int64, 6, 0, True, DEFAULT_EMB_DIM, False, 4),
    #     Feature("VolumeControlPriority", tf.int64, 8, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("UserHourOfWeek", tf.int64, 24 * 7 + 2, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("AdsTxtSellerType", tf.int64, 7, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("PublisherType", tf.int64, 7, 0, True, DEFAULT_EMB_DIM, False, 4),
    #     Feature("InternetConnectionType", tf.int64, 6, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("DeviceType", tf.int64, 9, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("OperatingSystemFamily", tf.int64, 10, 0, True, DEFAULT_EMB_DIM, False, 4),
    Feature("Browser", tf.int64, 20, 0, True, DEFAULT_EMB_DIM, False, 4)
]

Target = namedtuple("Feature", "name, type, default_value, enabled")

model_targets = [
    Target("is_imp", tf.float32, None, True),
    Target("AuctionBidPrice", tf.float32, None, True),
    Target("RealMediaCost", tf.float32, 0.0, True),
    Target("mb2w", tf.float32, None, True),
    Target("FloorPriceInUSD", tf.float32, 0.0, True)
]


feature_description_ints = {f.name: tf.io.FixedLenFeature([], f.type, f.default_value) for f in model_features}
feature_description_ints.update({t.name: tf.io.FixedLenFeature([], t.type, t.default_value) for t in model_targets})

#
# for f in model_features:
#     print(
#         f"{f.name}, vocab_size={f.cardinality} emb_dim={f.embedding_dim} num_collisions={f.qr_collisions}, feat_dtype={f.type}")


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


def get_dataset_from_files(files, batch_size):
    return tf.data.TFRecordDataset(
        files,
        compression_type="GZIP",
    ).batch(
        batch_size=batch_size,
        drop_remainder=True
    ).map(
        parse_just_ints,
        num_parallel_calls=tf.data.AUTOTUNE,
        deterministic=False
    ).prefetch(
        10
    )
