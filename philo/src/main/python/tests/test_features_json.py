import json
import jsonschema
import pytest

from jsonschema import validate
from dalgo_utils.schema import schema

FEATURES_JSON = "src/main/python/features.json"

def test_features_json():
    with open(FEATURES_JSON, 'r') as f:
        json_data = json.load(f)

    try:
        validate(instance=json_data, schema=schema)
    except jsonschema.SchemaError as e:
        pytest.fail(f"features.json schema invalid: {e}")
    except jsonschema.ValidationError as e:
        pytest.fail(f"Error validating features.json: {e}")
    except Exception as e:
        pytest.fail(f"Exception while validating features.json: {e}")
