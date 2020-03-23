# dynamodb-parallel-scanner

This is just a small utility for performing parallel scans
on a `DynamoDB` table. This module allows for scans to be executed
in parallel with an api that prevents too much memory from
being pulled into memory.

## Installation

```bash
npm i dynamodb-parallel-scanner
```

## Usage

This module exposes a `parallelScan` function that accepts a
`DynamoDB` documentClient, a base `ScanInput`, the number of `totalSegments`
to break split up the scan with, and some hooks for reacting to results
and errors.

Example Usage:

```js
import DynamoDB from 'aws-sdk/clients/dynamodb';
const dynamodb = new DynamoDB();
const documentClient = new DynamoDB.DocumentClient({ service: dynamodb });

await parallelScan({
  // an input documentClient (configured how ever you like)
  documentClient,
  // the input you would normally pass to documentClient.scan
  // (without the TotalSegments and Segments options specified)
  scanInput: {
    TableName: 'my-table',
  },

  // a callback for reacting to the results of a scan
  //
  // note: this onResults hook will block to scheduling of the next scan
  // for the current context.segment to prevent pulling too much data
  // into memory
  onResults: async (context, scanResult) => {
    console.log(context.segment); // the segment that we are scanning

    try {
      await doSomethingWithItems(scanResult.Items);
    } catch (err) {
      console.error(err);
      context.abort(); // error occurred, we can abort the execution if you'd like
    }
  },
  // a callback for reacting to errors that might occur
  onError: (context, err) => {
    console.log(context.segment); // the segment that we are scanning

    console.error(err);

    context.abort(); // error occurred, we can abort the execution if you'd like
  },

  // the number of segments to scan in parallel
  // this module will default to a value of 1 if this option is not specified
  totalSegments: 5,

  // scans that fail are automatically retried
  // This module uses the @lifeomic/attempt library
  // for handling retries. Please see https://github.com/lifeomic/attempt
  // for more information on how scan retry logic can be tuned.
  //
  // NOTE: this will not retry failures from the `onResults` function
  // it's up to you to decide how you want to do that (if you want)
  attemptOptions: {
    maxAttempts: 2,
  },
});
```

## Testing

This module will assume you have `docker` and `docker-compose` installed.
To run tests, simply run `npm test`. This will start a dynamodb container and
run tests against that locally. To close the container,
run `docker-compose down` or `npm docker:down`.
