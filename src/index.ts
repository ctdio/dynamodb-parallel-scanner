import DynamoDB from 'aws-sdk/clients/dynamodb';
import { retry, PartialAttemptOptions } from '@lifeomic/attempt';
import PromiseQueue from 'p-queue';

const DEFAULT_TOTAL_SEGMENTS = 1;

export interface Scanner {
  start: () => void;
  abort: () => void;
  onIdle: () => void;
}

export type ScanOutput = DynamoDB.DocumentClient.ScanOutput;

export interface ScanInput {
  documentClient: DynamoDB.DocumentClient;
  scanInput: Omit<
    DynamoDB.DocumentClient.ScanInput,
    'Segment' | 'TotalSegments'
  >;
  onResults: (context: ScanContext, output: ScanOutput) => void | Promise<void>;
  onError?: (context: ScanContext, err: Error) => void | Promise<void>;
  totalSegments?: number;
  attemptOptions?: PartialAttemptOptions<ScanOutput | null>;
}

export interface ScanContext {
  segment: number;
  abort: Scanner['abort'];
  lastEvaluatedKey?: DynamoDB.DocumentClient.Key;
}

export function parallelScan(input: ScanInput): Promise<void> {
  const scanner = createScanner(input);
  scanner.start();
  return scanner.onIdle();
}

export function createScanner({
  documentClient,
  scanInput,
  onResults,
  onError,
  attemptOptions,
  totalSegments = DEFAULT_TOTAL_SEGMENTS,
}: ScanInput) {
  const workerQueue = new PromiseQueue({ concurrency: totalSegments });

  let started = false;
  let aborted = false;

  const scanner = {
    start: () => {
      if (!started) {
        started = true;
        for (let i = 0; i < totalSegments; i++) {
          const segment = i;
          // kick off work for each segment
          workerQueue.add(() => executeScan(segment));
        }
      }
    },
    abort: () => {
      aborted = true;
      workerQueue.clear();
    },
    onIdle: () => workerQueue.onIdle(),
  };

  async function executeScan(
    segment: number,
    lastEvaluatedKey?: DynamoDB.DocumentClient.Key
  ) {
    if (aborted) {
      return;
    }

    const executionContext: ScanContext = {
      abort: scanner.abort,
      segment,
      lastEvaluatedKey,
    };

    const input: DynamoDB.DocumentClient.ScanInput = {
      ...scanInput,
      TotalSegments: totalSegments,
      Segment: segment,
      ExclusiveStartKey: lastEvaluatedKey,
    };

    try {
      const scanOutput = await retry<ScanOutput | null>(
        () =>
          aborted
            ? Promise.resolve(null)
            : documentClient.scan(input).promise(),
        attemptOptions
      );

      // skip work if abort was called while data was being fetched
      if (!aborted && scanOutput !== null) {
        await onResults(executionContext, scanOutput);
      }

      // stage next set of work if there is more data
      if (!aborted && scanOutput?.LastEvaluatedKey) {
        workerQueue.add(() =>
          executeScan(segment, scanOutput.LastEvaluatedKey)
        );
      }
    } catch (err) {
      await onError?.(executionContext, err);
    }
  }

  return scanner;
}
