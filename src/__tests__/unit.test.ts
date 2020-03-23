import DynamoDB from 'aws-sdk/clients/dynamodb';
import { parallelScan, ScanContext } from '..';

const documentClient = new DynamoDB.DocumentClient();

afterEach(() => {
  jest.clearAllMocks();
});

test('should kick off scans for the number of totalSegments', async () => {
  const scanSpy = jest.spyOn(documentClient, 'scan').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [{ id: 'test' }],
      }),
  } as any);

  const scanInput = { TableName: 'test' };
  const onResultsSpy = jest.fn();

  await parallelScan({
    documentClient,
    scanInput,
    onResults: onResultsSpy,
    totalSegments: 5,
  });

  expect(onResultsSpy).toHaveBeenCalledTimes(5);
  expect(scanSpy).toHaveBeenCalledTimes(5);

  for (let i = 0; i < 5; i++) {
    expect(scanSpy).toHaveBeenNthCalledWith(i + 1, {
      ...scanInput,
      Segment: i,
      TotalSegments: 5,
    });
  }
});

test('should pass along last evaluted key after successful runs', async () => {
  const mockLastEvaluatedKey = { test: 1 };
  const scanSpy = jest.spyOn(documentClient, 'scan').mockReturnValueOnce({
    promise: () =>
      Promise.resolve({
        Items: [{ id: 'test' }],
        LastEvaluatedKey: mockLastEvaluatedKey,
      }),
  } as any);

  const scanInput = { TableName: 'test' };
  const onResultsSpy = jest.fn();

  await parallelScan({
    documentClient,
    scanInput,
    onResults: onResultsSpy,
  });

  expect(onResultsSpy).toHaveBeenCalledTimes(2);
  expect(scanSpy).toHaveBeenCalledTimes(2);

  const expectedScanInput = {
    ...scanInput,
    Segment: 0,
    TotalSegments: 1,
  };

  expect(scanSpy).toHaveBeenNthCalledWith(1, {
    ...expectedScanInput,
    ExclusiveStartKey: undefined,
  });
  expect(scanSpy).toHaveBeenNthCalledWith(2, {
    ...expectedScanInput,
    ExclusiveStartKey: mockLastEvaluatedKey,
  });
});

test('should retry failed scans', async () => {
  const scanSpy = jest
    .spyOn(documentClient, 'scan')
    .mockReturnValueOnce({
      promise: () => Promise.reject(new Error('error')),
    } as any)
    .mockReturnValueOnce({
      promise: () =>
        Promise.resolve({
          Items: [{ id: 'test' }],
        }),
    } as any);

  const scanInput = { TableName: 'test' };
  const onResultsSpy = jest.fn();

  await parallelScan({
    documentClient,
    scanInput,
    onResults: onResultsSpy,
  });

  expect(onResultsSpy).toHaveBeenCalledTimes(1);
  expect(scanSpy).toHaveBeenCalledTimes(2);

  const expectedScanInput = {
    ...scanInput,
    Segment: 0,
    TotalSegments: 1,
  };

  expect(scanSpy).toHaveBeenNthCalledWith(1, expectedScanInput);
  expect(scanSpy).toHaveBeenNthCalledWith(2, expectedScanInput);
});

test('should call error callback via options', async () => {
  const scanError = new Error('error');
  const scanSpy = jest.spyOn(documentClient, 'scan').mockReturnValue({
    promise: () => Promise.reject(scanError),
  } as any);

  const scanInput = { TableName: 'test' };
  const onResultsSpy = jest.fn();
  const onErrorSpy = jest.fn();

  await parallelScan({
    documentClient,
    scanInput,
    onResults: onResultsSpy,
    onError: onErrorSpy,
    attemptOptions: {
      maxAttempts: 3,
    },
  });

  expect(scanSpy).toHaveBeenCalledTimes(3); // matches maxAttempts

  expect(onErrorSpy).toHaveBeenCalledTimes(1);
  expect(onErrorSpy).toHaveBeenCalledWith(
    { segment: 0, abort: expect.any(Function) },
    scanError
  );
});

test('should allow for work to aborted', async () => {
  const scanSpy = jest.spyOn(documentClient, 'scan').mockReturnValue({
    promise: () =>
      Promise.resolve({
        Items: [{ id: 'test' }],
      }),
  } as any);

  const handlerError = new Error('Handler error');

  const scanInput = { TableName: 'test' };
  const onResultsSpy = jest.fn().mockRejectedValue(handlerError);
  const onErrorSpy = jest.fn((context: ScanContext) => context.abort());

  await parallelScan({
    documentClient,
    scanInput,
    onResults: onResultsSpy,
    onError: onErrorSpy,
  });

  expect(onErrorSpy).toHaveBeenCalledTimes(1);
  expect(onErrorSpy).toHaveBeenCalledWith(
    { segment: 0, abort: expect.any(Function) },
    handlerError
  );

  expect(scanSpy).toHaveBeenCalledTimes(1);
});
