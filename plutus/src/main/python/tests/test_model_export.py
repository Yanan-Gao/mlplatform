import unittest
from datetime import datetime
from pathlib import Path

import grpc
import tensorflow as tf
from tensorflow_serving.apis import prediction_service_pb2_grpc

from plutus import CpdType, ModelHeads
from plutus.data import generate_random_pandas, generate_random_grpc_query
from plutus.features import default_model_targets, default_model_features
from plutus.losses import google_fpa_nll
from plutus.models import dlrm_model


class ModelExportTestCase(unittest.TestCase):

    def test_model_export(self):
        self.model_features = default_model_features
        self.model_targets = default_model_targets
        self.n_observations = 1000
        self.batch_size = 10
        self.num_epochs = 2

        features, targets = generate_random_pandas(self.model_features, self.model_targets, self.n_observations)
        self.ds = tf.data.Dataset.from_tensor_slices((dict(features), targets)).batch(self.batch_size)

        model = dlrm_model(features=self.model_features,
                           activation="relu",
                           combiner=tf.keras.layers.Flatten(),
                           top_mlp_layers=[512, 256, 64],
                           bottom_mlp_layers=[512, 256, 64, 16],
                           cpd_type=CpdType.LOGNORM,
                           heads=ModelHeads.CPD,
                           mixture_components=3,
                           dropout_rate=None,
                           batchnorm=False)

        model.summary()

        model.compile(tf.keras.optimizers.Adam(), loss=google_fpa_nll)
        model.fit(self.ds, epochs=self.num_epochs, verbose=1)

        models_dir = Path(f'/tmp/{datetime.now().strftime("%Y%m%d/%H%M")}')
        models_dir.mkdir(parents=True, exist_ok=True)

        model.save(f"{models_dir.resolve().__str__()}/aaa/1")

        model2 = tf.keras.models.load_model(f"{models_dir.resolve().__str__()}/aaa/1")
        for x, y in self.ds.unbatch().batch(1).take(10):
            print(model2(x))

        params_model = tf.keras.Model(model2.input, model2.get_layer("params").output)
        params_model.save(f"{models_dir.resolve().__str__()}/aaa/2")

        for x, y in self.ds.unbatch().batch(1).take(10):
            print(params_model.predict(x))


    @unittest.skip("more of an integration test")
    def test_grpc_response(self):
        """
        Start Docker
        docker run -it --cpus=1 --rm --entrypoint=/bin/bash -p 8500:8500 -p 8501:8501 -v /tmp/20210908/1026/models/:/models -t tensorflow/serving:latest

        Single Model
        tensorflow_model_server --port=8500 --rest_api_port=0 --prefer_tflite_model --model_name=multi_output --model_base_path=/models/aaa

        Multiple/Versioned Models
         tensorflow_model_server --port=8500 --rest_api_port=0 --prefer_tflite_model --model_config_file=/models/model_config_list.conf --allow_version_labels_for_unavailable_models
         model_config_file
         ====================
         model_config_list: {
             config: {
                 name: "test",
                 base_path: "/models/aaa",
                 model_platform: "tensorflow",
                 model_version_policy: {
                     specific {
                         versions: 1
                         versions: 2
                     }
                 }
                 version_labels {
                   key: 'prod'
                   value: 1
                 }
                 version_labels {
                   key: 'test'
                   value: 2
                 }
             }
         }
        """

        def send_grpc_request(stub, req, timeout=10):
            return stub.Predict(req, timeout)

        channel = grpc.insecure_channel('0.0.0.0:8500')
        stub = prediction_service_pb2_grpc.PredictionServiceStub(channel)
        resp = send_grpc_request(stub, generate_random_grpc_query(default_model_features, "test", "test"))
        print(resp)


if __name__ == '__main__':
    unittest.main()
