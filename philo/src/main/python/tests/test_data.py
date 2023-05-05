# Created by jiaxing.pi at 3/21/23
import pytest
from philo.data import validate_and_generate_files_dict

TRAIN = "train"
VAL = "validation"
TEST = "test"


# test the validate_and_generate_files_dict function
@pytest.mark.parametrize(
    "files, expected_length_list, expected_message",
    [({TRAIN: ['train1', 'train2'], VAL: ['val1', 'val2', 'val3'], TEST: ['test1', 'test2']}, [2, 3, 2], None),
     ({TRAIN: ['train1', 'train2'], VAL: ['val1', 'val2', 'val3']}, [2, 1, 2], None),
     ({TRAIN: ['train1', 'train2'], VAL: ['val1', 'val2', 'val3'], TEST: []}, [2, 1, 2], None),
     ({TRAIN: ['train1', 'train2'], TEST: ['test1', 'test2', 'test3']}, [2, 1, 2], None),
     ({TRAIN: ['train1', 'train2'], VAL: [], TEST: ['test1', 'test2', 'test3']}, [2, 1, 2], None),
     ({}, None, 'No files found in the input path'),
     ({TRAIN: [], VAL: [], TEST: []}, None, 'No files found in the input path'),
     ({TRAIN: ['train1']}, None, f"Only {TRAIN} found in the input path"),
     ({TRAIN: ['train1'], VAL: [], TEST: []}, None, f"Only {TRAIN} found in the input path"),
     ({VAL: ['val1', 'val2', 'val3'], TEST: ['test1', 'test2']}, None, 'No training data found in the input path'),
     ({TRAIN: [], VAL: ['val1', 'val2', 'val3'], TEST: ['test1', 'test2']}, None, 'No training data found in the input path')
     ]
)
def test_validate_and_generate_files_dict(files, expected_length_list, expected_message):
    if expected_message is not None:
        with pytest.raises(Exception, match=expected_message):
            validate_and_generate_files_dict(files)
    else:
        files = validate_and_generate_files_dict(files)
        train = files[TRAIN]
        validate = files[VAL]
        test = files[TEST]
        assert len(train) == expected_length_list[0]
        assert len(validate) == expected_length_list[1]
        assert len(test) == expected_length_list[2]
