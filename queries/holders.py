"""
queries/holders.py
-------------------
GraphQL queries for token holder snapshots.
Used for: concentration metrics, bundle detection, dev holdings tracking.
"""

# ── 1. Top holders of a token (latest balance snapshot) ───────────────────
TOP_HOLDERS = """
query TopHolders($mint: String!, $limit: Int!) {
  Solana {
    BalanceUpdates(
      where: {
        BalanceUpdate: {
          Currency: { MintAddress: { is: $mint } }
        }
      }
      orderBy: { descending: BalanceUpdate_PostBalance }
      limit: { count: $limit }
    ) {
      BalanceUpdate {
        Account { Address }
        PostBalance
        PreBalance
        Currency { MintAddress  Symbol }
      }
      Block { Time }
    }
  }
}
"""

# ── 2. Specific wallet balance for a token ────────────────────────────────
WALLET_BALANCE = """
query WalletBalance($mint: String!, $wallet: String!) {
  Solana {
    BalanceUpdates(
      where: {
        BalanceUpdate: {
          Currency: { MintAddress: { is: $mint } }
          Account: { Address: { is: $wallet } }
        }
      }
      orderBy: { descending: Block_Time }
      limit: { count: 1 }
    ) {
      BalanceUpdate {
        PostBalance
        Account { Address }
      }
      Block { Time }
    }
  }
}
"""

# ── 3. All balance updates for a token (full transfer history) ────────────
# Heavier query: gives the temporal evolution of holdings
BALANCE_HISTORY = """
query BalanceHistory($mint: String!, $after: DateTime!, $before: DateTime!) {
  Solana {
    BalanceUpdates(
      where: {
        BalanceUpdate: {
          Currency: { MintAddress: { is: $mint } }
        }
        Block: { Time: { after: $after, before: $before } }
      }
      orderBy: { ascending: Block_Time }
      limit: { count: 1000 }
    ) {
      Block { Time  Slot }
      BalanceUpdate {
        Account { Address }
        PreBalance
        PostBalance
        Amount
      }
      Transaction { Signature  Signer }
    }
  }
}
"""
