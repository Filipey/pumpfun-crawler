"""
queries/tokens.py
------------------
GraphQL queries for token creation events and metadata.
Used for: Axis 1 (multi-label taxonomy) and Axis 2 (creator network).
"""

from config import PUMPFUN_PROGRAM

# ── 1. Batch: all token creations in a time window ────────────────────────
# Returns: creator address, mint address, name, symbol, URI (for image/desc),
#          timestamp, decimals, metadata address
TOKEN_CREATIONS = f"""
query TokenCreations($after: DateTime!, $before: DateTime!) {{
  Solana {{
    TokenSupplyUpdates(
      where: {{
        Instruction: {{
          Program: {{
            Address: {{ is: "{PUMPFUN_PROGRAM}" }}
            Method: {{ in: ["create", "create_v2"] }}
          }}
        }}
        Block: {{ Time: {{ after: $after, before: $before }} }}
        Transaction: {{ Result: {{ Success: true }} }}
      }}
      orderBy: {{ ascending: Block_Time }}
      limit: {{ count: 1000 }}
    ) {{
      Block {{
        Time
        Height
      }}
      Transaction {{
        Signer
        Signature
      }}
      TokenSupplyUpdate {{
        Amount
        Currency {{
          Symbol
          Name
          MintAddress
          MetadataAddress
          Uri
          Decimals
          Fungible
          UpdateAuthority
        }}
        PostBalance
      }}
    }}
  }}
}}
"""

# ── 2. All tokens created by a specific dev address ───────────────────────
# Used for: dev clustering, measuring dev prolificacy
TOKENS_BY_CREATOR = f"""
query TokensByCreator($creator: String!) {{
  Solana {{
    TokenSupplyUpdates(
      where: {{
        Transaction: {{
          Result: {{ Success: true }}
          Signer: {{ is: $creator }}
        }}
        Instruction: {{
          Program: {{
            Address: {{ is: "{PUMPFUN_PROGRAM}" }}
            Method: {{ in: ["create", "create_v2"] }}
          }}
        }}
      }}
      orderBy: {{ ascending: Block_Time }}
      limit: {{ count: 1000 }}
    ) {{
      Block {{ Time  Height }}
      TokenSupplyUpdate {{
        Currency {{
          Symbol
          Name
          MintAddress
          Uri
        }}
      }}
      Transaction {{ Signature  Signer }}
    }}
  }}
}}
"""

# ── 3. Creation time + dev address for a specific mint ────────────────────
TOKEN_CREATION_DETAIL = f"""
query TokenDetail($mint: String!) {{
  Solana(network: solana) {{
    Instructions(
      where: {{
        Instruction: {{
          Accounts: {{ includes: {{ Address: {{ is: $mint }} }} }}
          Program: {{
            Name: {{ is: "pump" }}
            Method: {{ in: ["create", "create_v2"] }}
          }}
        }}
      }}
    ) {{
      Block {{ Time }}
      Transaction {{ Signer  Signature }}
      Instruction {{
        Accounts {{ Address }}
        Program {{
          Arguments {{
            Name
            Value {{
              ... on Solana_ABI_String_Value_Arg {{ string }}
              ... on Solana_ABI_Address_Value_Arg {{ address }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# ── 4. Top creators by number of tokens launched ──────────────────────────
TOP_CREATORS = f"""
query TopCreators($after: DateTime!, $before: DateTime!) {{
  Solana {{
    TokenSupplyUpdates(
      where: {{
        Instruction: {{
          Program: {{
            Address: {{ is: "{PUMPFUN_PROGRAM}" }}
            Method: {{ in: ["create", "create_v2"] }}
          }}
        }}
        Block: {{ Time: {{ after: $after, before: $before }} }}
        Transaction: {{ Result: {{ Success: true }} }}
      }}
      orderBy: {{ descendingByField: "token_count" }}
      limit: {{ count: 100 }}
    ) {{
      Transaction {{ Signer }}
      token_count: count(distinct: TokenSupplyUpdate_Currency_MintAddress)
    }}
  }}
}}
"""
