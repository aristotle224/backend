import { describe, it, expect, beforeEach, vi } from 'vitest';

// Stub out BullMQ queues so the service can be imported without a Redis connection.
const { mockBalanceVerificationQueueAdd } = vi.hoisted(() => ({
  mockBalanceVerificationQueueAdd: vi.fn(),
}));
vi.mock('../../../lib/queue', () => ({
  stellarConfirmationQueue: { add: vi.fn() },
  webhookDispatchQueue: { add: vi.fn() },
  balanceVerificationQueue: { add: mockBalanceVerificationQueueAdd },
}));

// Stub Stellar transaction helpers — not exercised in these tests.
vi.mock('../../../lib/stellar/transactions', () => ({
  buildPaymentTransaction: vi.fn(),
  submitSignedTransaction: vi.fn(),
  checkTransactionStatus: vi.fn(),
}));

// Stub getWalletBalance so tests stay hermetic (no Stellar network). Tip
// creation no longer calls this directly (#221); it is only still imported by
// tests that exercise the balance-verification worker elsewhere.
vi.mock('../../../lib/stellar/wallet', () => ({
  getWalletBalance: vi.fn(),
}));

import { PaymentService } from '../payment.service';

// ---------------------------------------------------------------------------
// Static vi.fn Prisma double — same shape used by tip-concurrency.test.ts
// ---------------------------------------------------------------------------

const mockPrisma = {
  creator: { findUnique: vi.fn(), update: vi.fn() },
  user: { findUnique: vi.fn() },
  wallet: { findFirst: vi.fn(), updateMany: vi.fn() },
  stellarAsset: { findFirst: vi.fn() },
  walletFlag: { findFirst: vi.fn() },
  accountFreeze: { findFirst: vi.fn() },
  webhook: { findMany: vi.fn() },
  tip: {
    create: vi.fn(),
    findUnique: vi.fn(),
    findMany: vi.fn(),
    count: vi.fn(),
    update: vi.fn(),
    updateMany: vi.fn(),
  },
  $transaction: vi.fn((callback: (tx: any) => Promise<unknown>) => callback(mockPrisma)),
};

/**
 * Regression tests for issue #221 (asynchronous payment processing).
 *
 * Tip creation used to call `getWalletBalance` (a Stellar Horizon RPC call)
 * synchronously and reject the request with 402/503 before ever touching the
 * database (issue #175's ordering). #221 replaces that: the balance check
 * moves to a queued job (`balanceVerificationQueue`, see
 * `src/lib/workers/__tests__/balance-verification.worker.test.ts` for its own
 * coverage) so tip creation itself can no longer time out or fail on a slow
 * or unreachable Stellar network. These tests assert the new contract:
 * creation always succeeds (given otherwise-valid input), returns a jobId,
 * and enqueues exactly one verification job carrying the data that job needs.
 */
describe('PaymentService.createTip — asynchronous creation (#221)', () => {
  const userId = 'user-balance';
  const creatorId = 'creator-ok';
  let service: PaymentService;

  beforeEach(() => {
    vi.clearAllMocks();
    service = new PaymentService(mockPrisma as any);
    mockPrisma.webhook.findMany.mockResolvedValue([]);

    mockPrisma.user.findUnique.mockResolvedValue({ id: userId });
    mockPrisma.creator.findUnique.mockResolvedValue({
      id: creatorId,
      userId: 'other-user',
      isPublic: true,
      verified: true,
    });
    mockPrisma.wallet.findFirst.mockResolvedValue({
      id: 'wallet-1',
      publicKey: 'GABC123',
      verified: true,
      balance: 1000,
      balanceVersion: 0,
    });
    mockPrisma.walletFlag.findFirst.mockResolvedValue(null);
    mockPrisma.accountFreeze.findFirst.mockResolvedValue(null);
    mockPrisma.stellarAsset.findFirst.mockResolvedValue({
      id: 'asset-usdc',
      code: 'USDC',
      issuer: 'GUSDCISSUER',
      enabled: true,
      priority: 100,
    });
    mockPrisma.tip.findUnique.mockResolvedValue(null);
    mockBalanceVerificationQueueAdd.mockResolvedValue({ id: 'job-1' });
  });

  function mockTipCreate(overrides: Record<string, unknown> = {}) {
    mockPrisma.tip.create.mockResolvedValue({
      id: 'tip-new',
      fromUserId: userId,
      creatorId,
      amount: 100,
      message: null,
      status: 'pending',
      transactionHash: null,
      assetCode: 'USDC',
      assetIssuer: 'GUSDCISSUER',
      assetDecimals: 7,
      moderationState: 'visible',
      createdAt: new Date(),
      updatedAt: new Date(),
      idempotencyKey: null,
      media: [],
      ...overrides,
    });
  }

  it('creates the tip immediately without calling getWalletBalance (no synchronous network call on the request path)', async () => {
    mockTipCreate();
    const { getWalletBalance } = await import('../../../lib/stellar/wallet');

    const result = await service.createTip(userId, { creatorId, amount: 100 });

    expect(result.id).toBe('tip-new');
    expect(result.status).toBe('pending');
    expect(mockPrisma.tip.create).toHaveBeenCalledTimes(1);
    expect(getWalletBalance).not.toHaveBeenCalled();
  });

  it('returns a jobId for the queued balance-verification job', async () => {
    mockTipCreate();

    const result = await service.createTip(userId, { creatorId, amount: 100 });

    expect(result.jobId).toBe('job-1');
  });

  it('enqueues exactly one balance-verification job carrying the tip id, wallet, asset and amount', async () => {
    mockTipCreate({ amount: 42 });

    await service.createTip(userId, { creatorId, amount: 42 });

    expect(mockBalanceVerificationQueueAdd).toHaveBeenCalledTimes(1);
    const [jobName, payload] = mockBalanceVerificationQueueAdd.mock.calls[0];
    expect(jobName).toBe('verify-balance');
    expect(payload).toMatchObject({
      tipId: 'tip-new',
      walletPublicKey: 'GABC123',
      assetCode: 'USDC',
      requiredAmount: 42,
    });
  });

  it('still creates the tip (degrading gracefully) when enqueueing the verification job fails', async () => {
    mockTipCreate();
    mockBalanceVerificationQueueAdd.mockRejectedValue(new Error('Redis unreachable'));

    const result = await service.createTip(userId, { creatorId, amount: 100 });

    // The whole point of #221: a queue/infra hiccup must never turn into a
    // failed tip-creation request the way a slow Stellar RPC call used to.
    expect(result.id).toBe('tip-new');
    expect(result.status).toBe('pending');
    expect(result.jobId).toBeUndefined();
  });

  it('still rejects an unverified-wallet caller before ever reaching the queue (existing validation preserved)', async () => {
    mockPrisma.wallet.findFirst.mockResolvedValue(null);

    await expect(service.createTip(userId, { creatorId, amount: 100 })).rejects.toMatchObject({
      statusCode: 400,
    });

    expect(mockPrisma.tip.create).not.toHaveBeenCalled();
    expect(mockBalanceVerificationQueueAdd).not.toHaveBeenCalled();
  });

  it('still rejects a flagged wallet before ever reaching the queue (existing validation preserved)', async () => {
    mockPrisma.walletFlag.findFirst.mockResolvedValue({ id: 'flag-1' });

    await expect(service.createTip(userId, { creatorId, amount: 100 })).rejects.toMatchObject({
      code: 'PAYMENT_DECLINED',
    });

    expect(mockPrisma.tip.create).not.toHaveBeenCalled();
    expect(mockBalanceVerificationQueueAdd).not.toHaveBeenCalled();
  });

  it('idempotent replay (existing tip for the same key) returns without enqueueing a new verification job', async () => {
    mockPrisma.tip.findUnique.mockResolvedValue({
      id: 'tip-existing',
      fromUserId: userId,
      creatorId,
      amount: 100,
      message: null,
      status: 'pending',
      transactionHash: null,
      assetCode: 'USDC',
      assetIssuer: 'GUSDCISSUER',
      assetDecimals: 7,
      createdAt: new Date(),
      updatedAt: new Date(),
      media: [],
    });

    const result = await service.createTip(userId, {
      creatorId,
      amount: 100,
      idempotencyKey: 'replay-key-123',
    });

    expect(result.id).toBe('tip-existing');
    expect(mockPrisma.tip.create).not.toHaveBeenCalled();
    expect(mockBalanceVerificationQueueAdd).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Stateful Prisma double that honours optimistic locking on the Wallet model,
// for the concurrent-tips test (#186).
// ---------------------------------------------------------------------------

function createConcurrentTipPrisma(userId: string, creatorId: string) {
  const state = {
    wallet: {
      id: 'wallet-1',
      publicKey: 'GABC123',
      userId,
      verified: true,
      balance: 100,
      balanceVersion: 0,
    },
    tipsCreated: 0,
  };

  const prisma = {
    user: { findUnique: vi.fn(async () => ({ id: userId })) },
    creator: { findUnique: vi.fn(async () => ({ id: creatorId, userId: 'other', isPublic: true, verified: true })) },
    wallet: {
      findFirst: vi.fn(async () => ({ ...state.wallet })),
      findUnique: vi.fn(async ({ where }: any) => {
        if (where.id !== state.wallet.id) return null;
        return { ...state.wallet };
      }),
      updateMany: vi.fn(async ({ where, data }: any) => {
        if (where.id !== state.wallet.id || where.balanceVersion !== state.wallet.balanceVersion) {
          return { count: 0 };
        }
        state.wallet.balance -= data.balance?.decrement ?? 0;
        state.wallet.balanceVersion += data.balanceVersion?.increment ?? 1;
        return { count: 1 };
      }),
    },
    stellarAsset: { findFirst: vi.fn(async () => ({ id: 'asset-usdc', code: 'USDC', issuer: 'GUSDCISSUER', enabled: true, priority: 100 })) },
    walletFlag: { findFirst: vi.fn(async () => null) },
    accountFreeze: { findFirst: vi.fn(async () => null) },
    webhook: { findMany: vi.fn(async () => []) },
    tip: {
      create: vi.fn(async ({ data }: any) => {
        state.tipsCreated += 1;
        return {
          id: `tip-${state.tipsCreated}`,
          fromUserId: data.fromUserId,
          creatorId: data.creatorId,
          amount: data.amount,
          message: data.message,
          status: data.status,
          transactionHash: null,
          assetCode: data.assetCode,
          assetIssuer: data.assetIssuer,
          assetDecimals: data.assetDecimals,
          moderationState: 'visible',
          createdAt: new Date(),
          updatedAt: new Date(),
          idempotencyKey: data.idempotencyKey,
          media: [],
        };
      }),
      findUnique: vi.fn(),
      findMany: vi.fn(),
      count: vi.fn(),
      update: vi.fn(),
      updateMany: vi.fn(),
    },
    $transaction: vi.fn(async (fn: any) => fn(prisma)),
  };

  return { prisma, state };
}

describe('createTip concurrent balance reservation (#186)', () => {
  const userId = 'user-concurrent';
  const creatorId = 'creator-concurrent';
  let service: PaymentService;

  beforeEach(() => {
    vi.clearAllMocks();
    mockGetWalletBalance.mockResolvedValue({ lumens: '1000', usdc: '1000' });
  });

  it('lets only one of several concurrent tips for the same wallet succeed', async () => {
    const { prisma, state } = createConcurrentTipPrisma(userId, creatorId);
    service = new PaymentService(prisma as any);

    const results = await Promise.allSettled(
      Array.from({ length: 5 }, () =>
        service.createTip(userId, { creatorId, amount: 40 })
      )
    );

    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');

    // Wallet starts with 100; each tip reserves 40, so only two can succeed.
    expect(fulfilled).toHaveLength(2);
    expect(rejected).toHaveLength(3);
    for (const result of rejected) {
      const reason = (result as PromiseRejectedResult).reason;
      expect(reason.statusCode).toBeGreaterThanOrEqual(400);
      expect(['CONFLICT', 'PAYMENT_REQUIRED']).toContain(reason.code);
    }

    expect(state.tipsCreated).toBe(2);
    expect(state.wallet.balance).toBe(20);
    expect(state.wallet.balanceVersion).toBe(2);
  });

  it('rejects with a clear conflict error when the balance is exhausted concurrently', async () => {
    const { prisma } = createConcurrentTipPrisma();
    service = new PaymentService(prisma as any);

    // Single tip that exactly exhausts the balance succeeds.
    await service.createTip(userId, { creatorId, amount: 100 });

    // A second concurrent tip finds no funds remaining and fails.
    await expect(
      service.createTip(userId, { creatorId, amount: 1 })
    ).rejects.toMatchObject({
      statusCode: 402,
      code: 'PAYMENT_REQUIRED',
    });
  });
});
