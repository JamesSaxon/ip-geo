## Notes for creating the batch environment.

* Build the Docker container...
  ```
  docker build -t ipgeo -f Dockerfile .
  ```

* Go to Elastic Container Repository: https://console.aws.amazon.com/ecr/repositories?region=us-east-1
  * Create repository (geo), and disable immutability so we overwrite the environment image. > Create.
  * Then click on the repo and > View push commands: 
    ```
    aws configure --profile cdac
    aws ecr get-login-password --region us-east-1 --profile cdac | docker login --username AWS --password-stdin XX.dkr.ecr.us-east-1.amazonaws.com
    ```
    (Delete the keys thereafter, if not on a *completely* secure machine.)
    ```
    docker tag ipgeo:latest XX.dkr.ecr.us-east-1.amazonaws.com/geo:latest
    docker push XX.dkr.ecr.us-east-1.amazonaws.com/geo:latest
    ```

* Then go to AWS Batch, to create the queues, environments, and definitions.

