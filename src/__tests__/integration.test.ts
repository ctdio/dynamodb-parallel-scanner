import DynamoDB from 'aws-sdk/clients/dynamodb';
import { PromiseValue } from 'type-fest';
import { sortBy } from 'lodash';

import {
  documentClient,
  createTestContext,
} from './util/integrationTestContext';

import { parallelScan, ScanContext, ScanOutput } from '..';

let testContext: PromiseValue<ReturnType<typeof createTestContext>>;
let itemsInTable: DynamoDB.DocumentClient.ItemList;

beforeAll(async () => {
  testContext = await createTestContext();
  itemsInTable = await testContext.generateData(100);
});

afterAll(async () => {
  if (testContext) {
    await testContext.teardown();
  }
});

test('should allow for data to be collected in parallel', async () => {
  let collectedItems: DynamoDB.DocumentClient.ItemList = [];
  const onResultsSpy = jest.fn((context: ScanContext, output: ScanOutput) => {
    if (output.Items) {
      collectedItems = collectedItems.concat(output.Items);
    }
  });

  await parallelScan({
    documentClient,
    scanInput: {
      TableName: testContext.tableName,
      Limit: 25,
    },
    onResults: onResultsSpy,
    totalSegments: 4,
  });

  expect(collectedItems.length).toEqual(itemsInTable.length);
  expect(sortBy(collectedItems, 'id')).toEqual(sortBy(itemsInTable, 'id'));
});

test('should continue scan as long as there is an lastEvaluatedKey', async () => {
  let scannedCount = 0;
  const onResultsSpy = jest.fn((context: ScanContext, output: ScanOutput) => {
    if (output.ScannedCount) {
      scannedCount += output.ScannedCount;
    }
  });

  await parallelScan({
    documentClient,
    scanInput: {
      TableName: testContext.tableName,
      ExpressionAttributeNames: {
        '#id': 'id',
      },
      FilterExpression: '#id = cat',
      Limit: 25,
    },
    onResults: onResultsSpy,
    totalSegments: 4,
  });

  expect(scannedCount).toEqual(itemsInTable.length);
});
