# MLPlatform Repo
This repository was set up to create a place for TTD machine learning projects to live - in a way that allows data science to create products without additional engineering resources.

There is no strict language requirements though implementing new features/models in previously unsupported languages will require operational (engineering) work that should be considered.

###General Guidelines
* We are trying to avoid fat JARs - if you need a library to run your tools, please include it in the docker image or EMR cluster you intend to run your code on.

* We are trying to prevent one project's dependencies from impacting many others.  In general, try to include your references in the build closest to your project's code (not at the root sbt or maven project)

### Versioning
TBD - General versioning guidelines are being implemented.  This is a work in progress and this README file will be updated with additional info as it becomes available.

### Test Code
Test code is allowed in the users folder.  Please create your test folder using your name - e.g. "users/ryan.reynolds/"

### Creation of Features
TBD - Features will be pushed to S3 and registered with the feature store.  This is a work in progress and this README file will be updated with additional info as it becomes available.

### Scheduling of Jobs
Jobs should be scheduled using the airflow-dags repository.

### Developer Machine Setup
This will vary depending on which feature/model you are working on.  Notes will be added here.

