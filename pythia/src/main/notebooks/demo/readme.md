# Pythia for demographics

## Introduction
This collection of notebooks and modules comprises a deep-learning model to predict the Age and Gender of an impression at bid time. Included here are the ETL pipeline, modeling training and validation, testing, and analysis. For those who want to run this pipeline, I apologize for the messiness. It will be cleaned up when there's time!

## Instructions
Run the numbered notebooks in the order that they are presented. 

The first notebook, "0_owdi-provider-consistency," looks at metrics published by the OWDI team to pick the best providers for demographic data, which will form our seed labels to be joined with geronimo. Currently, we override the logic and assign `lucid` as the provider for the US.

Notebooks "0_owdi_preprocess", "12", and "3" should be chained together in one databricks job, see Job ID `419116735372850`. Currently, Geronimo processes data in 1 hour buckets, so we have 24 tasks for each hourPart. From the given date entered in the databricks widget field, we look backwards to process five days worth of sequential data; 3 for training, 1 for validation, and 1 (the most recent date) for testing. The test date should generate both the resampled set (`make_test_set=False`) as well as the unbalanced, unresampled set (`make_test_set=True`). Notebook "3" is a scala notebook that calls the scala file `pythia_module`, which goes from intermediate output to tfrecords format.

Notebooks "4" and "5" can be chained together into one job, see Job ID `199042719064780`. The deep learning model is enclosed in `demo_nn.py`, along with TabNet. Notebook "5" is the notebook for test inference, and the flag `drop_power_users_override` will trigger test inference on the resampled dataset or the full test dataset (`make_test_set=True` from the ETL pipeline).

The output and other intermediate files will be written to:

s3://thetradedesk-mlplatform-us-east-1/features/data/pythia/demo/

s3://thetradedesk-mlplatform-us-east-1/models/dev/pythia/demoModel/

Notebook "6" will produce a dataframe of F-scores and figures of the distribution vs. the test set, saved to `metrics` under `demoModel`

## Parameters
Each notebook will have documentation on the parameters for ETL, training, and testing.

## Notes and todo
- Unfortunately, the paths are scattered around the notebooks.
- Should eventually turn notebooks into python/scala files for Airflow
- Train a model by region
- Consider ordinal classification / regression for AgeGender and Age models
- Various explorations of hyperparameters to improve performance