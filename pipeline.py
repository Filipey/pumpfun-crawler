"""
pumpfun_collector/pipeline.py
------------------------------
Main orchestrator with TWO collection backends:

  1. DUNE (--backend dune): retroactive bulk collection, FREE
  2. BITQUERY (--backend bitquery): continuous daily collection, FREE TIER
  3. BOTH (--backend both): Dune retroactive + Bitquery forward

Usage examples:
    python pipeline.py --backend dune --start 2024-01-01 --end 2024-07-01
    python pipeline.py --backend dune --dune-mode manual --start 2024-01-01 --end 2024-07-01
    python pipeline.py --dune-import-csv
    python pipeline.py --backend bitquery --start 2025-06-01 --end 2025-06-02
    python pipeline.py --only network
    python pipeline.py --only metadata --max-tokens 5000
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from config import PARQUET_DIR
from utils import date_range_days, load_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")


def parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


# ═══════════════ DUNE BACKEND (retroactive, free) ═════════════════════════

def run_dune_collection(start, end, mode="api", query_types=None):
    from collectors.dune.bulk_collector import DuneBulkCollector
    from collectors.dune.dune_runner import DuneRunner

    runner = DuneRunner()
    collector = DuneBulkCollector(runner)

    if mode == "manual":
        types = query_types or "all"
        collector.generate_sql_files(start, end, query_type=types)
        logger.info("SQL files generated. See instructions above.")
        return

    logger.info("=" * 60)
    logger.info("DUNE RETROACTIVE COLLECTION (API mode)")
    logger.info(f"Period: {start.date()} -> {end.date()}")
    logger.info("=" * 60)

    types = query_types or ["creations", "trades", "migrations", "stats"]
    if isinstance(types, str):
        types = [types]

    if "creations" in types:
        logger.info("Phase 1: Token creations...")
        collector.collect_token_creations(start, end)

    if "migrations" in types:
        logger.info("Phase 2: Migrations...")
        collector.collect_migrations(start, end)

    if "stats" in types:
        logger.info("Phase 3: Token summary stats...")
        collector.collect_token_stats(start, end)

    if "creators" in types:
        logger.info("Phase 4: Top creators...")
        collector.collect_top_creators(start, end)

    if "trades" in types:
        logger.info("Phase 5: All trades (this may take a while)...")
        collector.collect_all_trades(start, end)

    logger.info("Dune collection complete.")


def run_dune_import_csv():
    from collectors.dune.bulk_collector import DuneBulkCollector
    collector = DuneBulkCollector()
    results = collector.import_manual_csvs()
    for dtype, df in results.items():
        logger.info(f"Imported {dtype}: {len(df)} rows")


# ═══════════════ BITQUERY BACKEND (continuous, free tier) ══════════════════════

def run_bitquery_collection(start, end, phase="all", mint_list=None,
                            wallet_list=None, max_tokens=500, max_wallets=200):
    from client import BitqueryClient
    from collectors.holder_collector import HolderCollector
    from collectors.migration_collector import MigrationCollector
    from collectors.token_collector import TokenCollector
    from collectors.trade_collector import TradeCollector

    client = BitqueryClient()

    if phase in ("tokens", "all"):
        logger.info("[Bitquery] Collecting token creations...")
        tc = TokenCollector(client)
        for day_start, day_end in date_range_days(start, end):
            tc.collect_creations(day_start, day_end)

    if phase in ("migrations", "all"):
        logger.info("[Bitquery] Collecting migrations...")
        mc = MigrationCollector(client)
        mc.collect_migrations(start, end)

    if phase in ("trades", "all"):
        logger.info("[Bitquery] Collecting trades...")
        tc = TradeCollector(client)
        mints = mint_list or _get_mints_from_collected()
        for mint in tqdm(mints[:max_tokens], desc="Trades"):
            try:
                tc.collect_token_trades(mint, start, end, window_minutes=30)
                tc.collect_first_buyers(mint)
            except Exception as e:
                logger.error(f"Error: {e}")

    if phase in ("holders", "all"):
        logger.info("[Bitquery] Collecting holders...")
        from collectors.holder_collector import HolderCollector
        hc = HolderCollector(client)
        mints = mint_list or _get_mints_from_collected()
        for mint in tqdm(mints[:max_tokens], desc="Holders"):
            try:
                hc.collect_top_holders(mint)
            except Exception as e:
                logger.error(f"Error: {e}")

    if phase in ("wallets", "all"):
        logger.info("[Bitquery] Profiling wallets...")
        tc = TradeCollector(client)
        wallets = wallet_list or _get_top_wallets()
        for wallet in tqdm(wallets[:max_wallets], desc="Wallets"):
            try:
                tc.collect_wallet_trades(wallet, start, end)
            except Exception as e:
                logger.error(f"Error: {e}")

    logger.info(f"[Bitquery] Complete. API calls: {client.call_count}")


# ═══════════════ SHARED PHASES ════════════════════════════════════════════

def run_network_build():
    from collectors.network_builder import NetworkBuilder
    logger.info("[Network] Building influence graph...")
    nb = NetworkBuilder()
    nb.build_creator_token_edges()
    nb.build_trader_token_edges()
    nb.build_wallet_cooccurrence(min_shared_tokens=2)
    G = nb.build_networkx_graph()
    logger.info(f"Network: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


def run_metadata_fetch(max_tokens=5000):
    from collectors.metadata_fetcher import MetadataFetcher
    logger.info(f"[Metadata] Fetching for up to {max_tokens} tokens...")
    mf = MetadataFetcher(download_images=True)
    mf.fetch_for_tokens(max_tokens=max_tokens)


# ═══════════════ HELPERS ══════════════════════════════════════════════════

def _get_mints_from_collected():
    mints = set()
    for d in [PARQUET_DIR / "tokens", PARQUET_DIR / "dune"]:
        for f in d.glob("creations_*.parquet"):
            try:
                df = pd.read_parquet(f)
                for col in ["mint_address", "mint"]:
                    if col in df.columns:
                        mints.update(df[col].dropna().tolist())
                        break
            except Exception:
                continue
    return list(mints)


def _get_top_wallets(top_n=200):
    wallets = {}
    for d in [PARQUET_DIR / "trades", PARQUET_DIR / "dune"]:
        if not d.exists():
            continue
        for f in d.glob("trades_*.parquet"):
            try:
                df = pd.read_parquet(f)
                col = next((c for c in ["trader_address", "trader_id"] if c in df.columns), None)
                if col:
                    for w, c in df[col].value_counts().items():
                        wallets[w] = wallets.get(w, 0) + c
            except Exception:
                continue
    return sorted(wallets, key=wallets.get, reverse=True)[:top_n]


def _load_file_list(path):
    p = Path(path)
    if p.exists():
        return [l.strip() for l in open(p) if l.strip()]
    return []


# ═══════════════ CLI ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="pump.fun collector: Dune (retroactive) + Bitquery (continuous)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--backend", choices=["dune", "bitquery", "both"], default="dune")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD")

    parser.add_argument("--dune-mode", choices=["api", "manual"], default="api")
    parser.add_argument("--dune-import-csv", action="store_true")
    parser.add_argument("--dune-types", type=str, default=None,
                        help="Comma-separated: creations,trades,migrations,stats,creators")

    parser.add_argument("--only", type=str, default=None,
                        choices=["tokens", "migrations", "trades", "holders",
                                 "wallets", "network", "metadata"])
    parser.add_argument("--mints", type=str)
    parser.add_argument("--wallets", type=str)
    parser.add_argument("--max-tokens", type=int, default=500)
    parser.add_argument("--max-wallets", type=int, default=200)

    args = parser.parse_args()

    if args.dune_import_csv:
        run_dune_import_csv()
        return
    if args.only == "network":
        run_network_build()
        return
    if args.only == "metadata":
        run_metadata_fetch(args.max_tokens)
        return

    if not args.start or not args.end:
        parser.error("--start and --end are required for collection")
    start, end = parse_date(args.start), parse_date(args.end)

    mint_list = _load_file_list(args.mints) if args.mints else None
    wallet_list = _load_file_list(args.wallets) if args.wallets else None
    dune_types = args.dune_types.split(",") if args.dune_types else None

    if args.backend in ("dune", "both"):
        run_dune_collection(start, end, mode=args.dune_mode, query_types=dune_types)

    if args.backend in ("bitquery", "both"):
        phase = args.only or "all"
        run_bitquery_collection(start, end, phase=phase,
                                mint_list=mint_list, wallet_list=wallet_list,
                                max_tokens=args.max_tokens, max_wallets=args.max_wallets)

    logger.info("Pipeline finished.")


if __name__ == "__main__":
    main()
