# xoc-utils-java
This library provides utils functions for xo-core java services, including Kafka client wrapper and data processor. 

## API References

### Kafka Clients

#### Consumer

#### ConsumerHandler

#### Producer

#### Stream

### Utils

#### generateKafkaMessage

## How to create a release？
#### Step 1: Create the build and upload
``` java
  ./gradlew build    // create the build
  ./gradlew publish   // release to maven market
```

#### Step 2: Release to the market 
Go to [Repository Manager](https://oss.sonatype.org) and log in,
you will find your repository in `Staging Repositories` list,
and follow the steps below:
1. Close the repository.
2. Refresh and wait until `Release` button is enabled
3. Click `Release` button to release,
and it will be available [here](https://oss.sonatype.org/content/groups/staging/io/xocore/xoc-utils-java/)
4. Wait, you will probably have to wait for a moment that no more than 2 hours until the library is available to download. 
