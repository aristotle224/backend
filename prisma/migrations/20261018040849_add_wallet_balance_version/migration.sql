-- Issue #186: optimistic locking for cached wallet balance.
--
-- Adds a `balance` column mirroring the on-chain wallet balance plus a
-- `balanceVersion` column used for compare-and-swap updates so concurrent tips
-- from the same wallet cannot both spend the same cached funds.

ALTER TABLE "Wallet"
  ADD COLUMN IF NOT EXISTS "balance" DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE "Wallet"
  ADD COLUMN IF NOT EXISTS "balanceVersion" INTEGER NOT NULL DEFAULT 0;

ANALYZE "Wallet";
