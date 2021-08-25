[[_TOC_]]

Feature Store (FS) uses a dockerized Spring REST API Application located on k8s

# Architecture
* namespace: mlplatform
  
* dev k8s cluster: general-use-non-production
  - URI: https://mlplatform.dev.gen.adsrvr.org/
* prod k8s cluster: TBD 
  - URI: TBD

# Passing credentials to our namespace via k8s secrets

1. create a Gitlab pipeline variable with the credential (or ask an mlplatform repo maintainer to do so)
2. create the secret during a CI job (specify in, eg. mlplatform/mlplatform/.gitlab-ci.yml )
    * example: 
    
`    - kubectl create secret -n mlplatform generic test-db-user-credentials --from-literal password=${MLPLATFORM_TEST_DB_PASSWORD} -o yaml --dry-run | kubectl apply -n mlplatform -f -
`
3. set environmental variables using your secret, when you build the docker container for mlplatform image to be deployed to k8s
mlplatform-api-deplopynent.yaml
   * example:
     
           env:
               name: MLPLATFORM_TEST_DB_PASSWORD
               valueFrom:
                 secretKeyRef:
                   name: test-db-user-credentials
                   key: password
    

# Contributions

All code *must* be reviewed by the team.

1. Create a pull request with your changes.
1. Get it reviewed by the team ([#scrum-aifun](https://thetradedesk.slack.com/archives/C01RMJ10G79)).
1. Arrange with the team for deployment.
