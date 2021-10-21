from unittest import TestCase
from datetime import datetime
from plutus.data import *


class TestData(TestCase):
    def test_copy_down_data(self):
        src_bkt = "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/modelinput/google/year=2021/month=8/day=21/lookback=7/format=tfrecord/validation/"
        dst_path = "/var/tmp/"
        result = copy_down_data(src_bkt, dst_path)
        print(result)

    def test_list_tfrecord_files(self):
        path = "/var/tmp/data/"
        files = list_tfrecord_files(path)
        # ds = tf.data.TFRecordDataset(files[0], compression_type="GZIP")
        # cnt = ds.reduce(np.int64(0), lambda x, _: x + 1)
        print(files)
        # print(cnt)

    def test_downsample(self):
        path = "/var/tmp/full/"
        files_dict = list_tfrecord_files(path)
        downsample_rate = 0.5
        downsampled_file_dict = downsample(files_dict, downsample_rate)
        print(downsampled_file_dict)

        expected = {
            "train": ['/private/var/tmp/full/train/train_3.gz', '/private/var/tmp/full/train/train_2.gz'],
            "validation": ['/private/var/tmp/full/validation/val_2.gz'],
            "test": ['/private/var/tmp/full/test/test_1.gz']
        }
        self.assertDictEqual(expected, downsampled_file_dict)

    def test_metadata(self):
        self.skipTest("not implemented yet")

    def test_model_creation_date(self):
        val = datetime.now().strftime("%Y%m%d%H")
        print(val)
        self.skipTest("nothing to test really")
