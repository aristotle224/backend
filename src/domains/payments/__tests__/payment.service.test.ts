import { describe, it, expect, beforeEach, vi } from 'vitest';

// Stub out BullMQ queues so the service can be imported without a Redis connection.
vi.mock('../../../lib/queue', () => ({
  stellarConfirmationQueue: { add: vi.fn() },
  webhookDispatchQueue: { add: vi.fn() },
}));

// Stub Stellar transaction helpers — not exercised in balance-check tests.
vi.mock('../../../lib/stellar/transactions', () => ({
  buildPaymentTransaction: vi.fn(),
  submitSignedTransaction: vi.fn(),
  checkTransactionStatus: vi.fn(),
}));

// Stub getWalletBalance so tests stay hermetic (no Stellar network).
vi.mock('../../../lib/stellar/wallet', () => ({
  getWalletBalance: vi.fn(),
}));

import { PaymentService } from '../payment.service';
import { PaymentRequiredError, ServiceUnavailableError } from '../../../utils/errors';
import { getWalletBalance } from '../../../lib/stellar/wallet';

const mockGetWalletBalance = getWalletBalance as ReturnType<typeof vi.fn>;

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

describe('PaymentService.createTip — balance checks (#175)', () => {
  const userId = 'user-balance';
  const creatorId = 'creator-ok';
  let service: PaymentService;

  beforeEach(() => {
    vi.clearAllMocks();
    service = new PaymentService(mockPrisma as any);
    mockPrisma.webhook.findMany.mockResolvedValue([]);

    // Default happy-path mocks shared by all tests in this suite
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

    // Default: wallet has ample balance — existing tests that run alongside pass.
    mockGetWalletBalance.mockResolvedValue({ lumens: '100', usdc: '1000' });
    mockPrisma.wallet.updateMany.mockResolvedValue({ count: 1 });
  });

  it('throws PaymentRequiredError (402) when wallet has insufficient USDC balance and creates no DB record', async () => {
    // USDC balance is only 5; tip amount is 100
    mockGetWalletBalance.mockResolvedValue({ lumens: '500', usdc: '5' });

    await expect(
      service.createTip(userId, { creatorId, amount: 100 })
    ).rejects.toMatchObject({
      statusCode: 402,
      code: 'PAYMENT_REQUIRED',
      details: { requiredAmount: 100, currentBalance: 5, currency: 'USDC' },
    });

    expect(mockPrisma.tip.create).not.toHaveBeenCalled();
  });

  it('throws PaymentRequiredError (402) when USDC balance is missing (undefined) and creates no DB record', async () => {
    // No USDC trustline — usdc field absent; treated as 0
    mockGetWalletBalance.mockResolvedValue({ lumens: '100' });

    await expect(
      service.createTip(userId, { creatorId, amount: 10 })
    ).rejects.toMatchObject({
      statusCode: 402,
      code: 'PAYMENT_REQUIRED',
    });

    expect(mockPrisma.tip.create).not.toHaveBeenCalled();
  });

  it('throws ServiceUnavailableError (503) when getWalletBalance fails and creates no DB record', async () => {
    mockGetWalletBalance.mockRejectedValue(new Error('Stellar network unreachable'));

    await expect(
      service.createTip(userId, { creatorId, amount: 50 })
    ).rejects.toMatchObject({ statusCode: 503 });

    expect(mockPrisma.tip.create).not.toHaveBeenCalled();
  });

  it('proceeds to create the tip when balance is exactly equal to amount', async () => {
    mockGetWalletBalance.mockResolvedValue({ lumens: '100', usdc: '25' });

    mockPrisma.tip.findUnique.mockResolvedValue(null);
    mockPrisma.tip.create.mockResolvedValue({
      id: 'tip-exact',
      fromUserId: userId,
      creatorId,
      amount: 25,
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
    });

    const result = await service.createTip(userId, { creatorId, amount: 25 });

    expect(result.id).toBe('tip-exact');
    expect(mockPrisma.tip.create).toHaveBeenCalledTimes(1);
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
