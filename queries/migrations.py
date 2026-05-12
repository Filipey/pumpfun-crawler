"""
queries/migrations.py
----------------------
GraphQL queries for token graduation events (pump.fun → PumpSwap).
Used for: identifying the changepoint between curve-phase and AMM-phase.
"""

from config import PUMPSWAP_PROGRAM

# ── 1. Batch: all migrations in a time window ─────────────────────────────
MIGRATIONS_BATCH = f"""
query Migrations($after: DateTime!, $before: DateTime!) {{
  Solana {{
    Instructions(
      where: {{
        Transaction: {{ Result: {{ Success: true }} }}
        Instruction: {{
          Depth: {{ eq: 1 }}
          Program: {{
            Method: {{ is: "create_pool" }}
            Address: {{ is: "{PUMPSWAP_PROGRAM}" }}
            Arguments: {{
              includes: [{{
                Value: {{
                  Address: {{
                    notIn: ["11111111111111111111111111111111"]
                  }}
                }}
              }}]
            }}
          }}
        }}
        Block: {{ Time: {{ after: $after, before: $before }} }}
      }}
      orderBy: {{ ascending: Block_Time }}
      limit: {{ count: 1000 }}
    ) {{
      Block {{
        Time
        Height
        Slot
      }}
      Transaction {{
        Signature
        Signer
      }}
      Instruction {{
        Accounts {{
          Address
          IsWritable
          Token {{
            Mint
            Owner
          }}
        }}
        Program {{
          Arguments {{
            Name
            Value {{
              ... on Solana_ABI_Address_Value_Arg {{ address }}
              ... on Solana_ABI_BigInt_Value_Arg {{ bigInteger }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# ── 2. Check if a specific token has migrated ─────────────────────────────
TOKEN_MIGRATION_STATUS = f"""
query MigrationStatus($mint: String!) {{
  Solana {{
    Instructions(
      where: {{
        Transaction: {{ Result: {{ Success: true }} }}
        Instruction: {{
          Depth: {{ eq: 1 }}
          Program: {{
            Method: {{ is: "create_pool" }}
            Address: {{ is: "{PUMPSWAP_PROGRAM}" }}
          }}
          Accounts: {{
            includes: [{{
              Token: {{ Mint: {{ is: $mint }} }}
            }}]
          }}
        }}
      }}
      limit: {{ count: 1 }}
    ) {{
      Block {{ Time  Slot }}
      Transaction {{ Signature }}
    }}
  }}
}}
"""
