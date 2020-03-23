import DynamoDB from 'aws-sdk/clients/dynamodb';
import { v4 as uuid } from 'uuid';
import { chunk, times } from 'lodash';
import pMap from 'p-map';

const dynamodb = new DynamoDB({
  apiVersion: '2012-08-10',
  region: 'us-east-1',
  endpoint: process.env.DYNAMODB_ENDPOINT || 'http://localhost:8000',
  accessKeyId: 'blah',
  secretAccessKey: 'blah',
});

export const documentClient = new DynamoDB.DocumentClient({
  service: dynamodb,
});

export async function createTestContext() {
  const tableName = uuid();
  const tableNameParam = {
    TableName: tableName,
  };

  await dynamodb
    .createTable({
      ...tableNameParam,
      AttributeDefinitions: [
        {
          AttributeName: 'id',
          AttributeType: 'S',
        },
      ],
      KeySchema: [
        {
          AttributeName: 'id',
          KeyType: 'HASH',
        },
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1,
      },
    })
    .promise();

  await dynamodb.waitFor('tableExists', tableNameParam).promise();

  return {
    tableName,
    generateData: (count?: number) => generateDataForTable(tableName, count),
    teardown: async () => {
      await dynamodb.deleteTable(tableNameParam).promise();
      await dynamodb.waitFor('tableNotExists', tableNameParam).promise();
    },
  };
}

async function generateDataForTable(tableName: string, count = 25) {
  const items = times(count, () => ({
    id: uuid(),
    test: uuid(),
  }));

  const batches = chunk(items, 25 /* dynamodb's max batch size*/);

  await pMap(
    batches,
    (batch) => {
      return documentClient
        .batchWrite({
          RequestItems: {
            [tableName]: batch.map((item) => ({
              PutRequest: { Item: item },
            })),
          },
        })
        .promise();
    },
    { concurrency: 2 }
  );

  return items;
}
