"""Master Trust instrument master: one public zip of five per-exchange CSVs.

CSV columns (all exchanges): exchange_token, trading_symbol, company_name,
close_price, expiry, strike, tick_size, lot_size, instrument_name, option_type,
segment, exchange, fin_instrm_pdct_tp_cd, asset_code.
"""

import io
import os
import zipfile

import pandas as pd
from sqlalchemy import Column, Float, Index, Integer, Sequence, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from broker.mastertrust.api.baseurl import CONTRACT_URL
from database.engine_factory import create_db_engine
from extensions import socketio
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

logger = get_logger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_db_engine(DATABASE_URL)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()


class SymToken(Base):
    __tablename__ = "symtoken"
    id = Column(Integer, Sequence("symtoken_id_seq"), primary_key=True)
    symbol = Column(String, nullable=False, index=True)
    brsymbol = Column(String, nullable=False, index=True)
    name = Column(String)
    exchange = Column(String, index=True)
    brexchange = Column(String, index=True)
    token = Column(String, index=True)
    expiry = Column(String)
    strike = Column(Float)
    lotsize = Column(Integer)
    instrumenttype = Column(String)
    tick_size = Column(Float)
    contract_value = Column(Float)

    __table_args__ = (Index("idx_symbol_exchange", "symbol", "exchange"),)


def init_db():
    logger.info("Initializing Master Contract DB")
    Base.metadata.create_all(bind=engine)


# Broker index names (uppercased) -> OpenAlgo symbols. Names not listed fall
# back to the uppercased name with spaces and hyphens removed ("Nifty Auto" ->
# NIFTYAUTO, "BHARATBOND-APR30" -> BHARATBONDAPR30).
NSE_INDEX_MAP = {
    "NIFTY 50": "NIFTY",
    # The underlying of the NFO NIFTYFPI contracts in this same master.
    "NIFTY FPI 150": "NIFTYFPI",
    "NIFTY NEXT 50": "NIFTYNXT50",
    "NIFTY FIN SERVICE": "FINNIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "NIFTY MID SELECT": "MIDCPNIFTY",
    "INDIA VIX": "INDIAVIX",
    "NIFTY DIV OPPS 50": "NIFTYDIVOPPS50",
    "NIFTY GS 10YR": "NIFTYGS10YR",
    "NIFTY GS 10YR CLN": "NIFTYGS10YRCLN",
    "NIFTY GS 11 15YR": "NIFTYGS1115YR",
    "NIFTY GS 15YRPLUS": "NIFTYGS15YRPLUS",
    "NIFTY GS 4 8YR": "NIFTYGS48YR",
    "NIFTY GS 8 13YR": "NIFTYGS813YR",
    "NIFTY GS COMPSITE": "NIFTYGSCOMPSITE",
}

BSE_INDEX_MAP = {
    "SNSX50": "SENSEX50",
    "SNXT50": "BSESENSEXNEXT50",
    "MID150": "BSE150MIDCAPINDEX",
    "LMI250": "BSE250LARGEMIDCAPINDEX",
    "MSL400": "BSE400MIDSMALLCAPINDEX",
    "AUTO": "BSEAUTO",
    "BSE CG": "BSECAPITALGOODS",
    "CARBON": "BSECARBONEX",
    "BSE CD": "BSECONSUMERDURABLES",
    "CPSE": "BSECPSE",
    "DOL100": "BSEDOLLEX100",
    "DOL200": "BSEDOLLEX200",
    "DOL30": "BSEDOLLEX30",
    "ENERGY": "BSEENERGY",
    "BSEFMC": "BSEFASTMOVINGCONSUMERGOODS",
    "FINSER": "BSEFINANCIALSERVICES",
    "GREENX": "BSEGREENEX",
    "BSE HC": "BSEHEALTHCARE",
    "INFRA": "BSEINDIAINFRASTRUCTUREINDEX",
    "INDSTR": "BSEINDUSTRIALS",
    "BSE IT": "BSEINFORMATIONTECHNOLOGY",
    "LRGCAP": "BSELARGECAP",
    "METAL": "BSEMETAL",
    "MIDCAP": "BSEMIDCAP",
    "MIDSEL": "BSEMIDCAPSELECTINDEX",
    "OILGAS": "BSEOIL&GAS",
    "POWER": "BSEPOWER",
    "REALTY": "BSEREALTY",
    "SMLCAP": "BSESMALLCAP",
    "SMLSEL": "BSESMALLCAPSELECTINDEX",
    "SMEIPO": "BSESMEIPO",
    "TECK": "BSETECK",
    "TELCOM": "BSETELECOM",
    # BSE short codes for indices in docs/prompt/symbol-format.md, named as
    # broker/fyers maps the same codes.
    "BHRT22": "BSEBHARAT22INDEX",
    "BSEEVI": "BSEENHANCEDVALUE",
    "BSELVI": "BSELOWVOLATILITY",
    "BSEMOI": "BSEMOMENTUM",
    "BSEPBI": "BSEPRIVATEBANKS",
    "BSEQUI": "BSEQUALITY",
    "BSHOSP": "BSEHOSPITALS",
    "CONDIS": "BSECONSUMERDISCRETIONARYGOODS&SERVICES",
    "ESG100": "BSE100ESG",
    "FOCIT": "BSEFOCUSEDIT",
    "LCTMCI": "BSE100LARGECAPTMCINDEX",
    "MFG": "BSEINDIAMANUFACTURING",
    "SML250": "BSE250SMALLCAPINDEX",
    "UTILS": "BSEUTILITIES",
}

COLUMNS = [
    "symbol",
    "brsymbol",
    "name",
    "exchange",
    "brexchange",
    "token",
    "expiry",
    "strike",
    "lotsize",
    "instrumenttype",
    "tick_size",
]


def _num(series, default=0.0):
    return pd.to_numeric(series, errors="coerce").fillna(default)


def _format_strike(strike):
    return str(int(strike)) if strike == int(strike) else str(strike)


def _frame(df, symbol, exchange, name, instrumenttype, expiry=""):
    out = pd.DataFrame(
        {
            "symbol": symbol,
            "brsymbol": df["trading_symbol"],
            "name": name,
            "exchange": exchange,
            "brexchange": df["exchange"],
            "token": df["exchange_token"].str.strip(),
            "expiry": expiry,
            "strike": 0.0,
            "lotsize": _num(df["lot_size"], 1).astype(int).clip(lower=1),
            "instrumenttype": instrumenttype,
            "tick_size": _num(df["tick_size"], 0.05),
        }
    )
    out.loc[out["tick_size"] <= 0, "tick_size"] = 0.05
    return out


def _derivatives(df, name):
    """FUT/CE/PE rows -> OpenAlgo symbols [name][DDMMMYY]FUT / [name][DDMMMYY][strike][CE|PE]."""
    expiry_dt = pd.to_datetime(df["expiry"], format="%d-%b-%Y", errors="coerce")
    df, name, expiry_dt = (
        df[expiry_dt.notna()],
        name[expiry_dt.notna()],
        expiry_dt[expiry_dt.notna()],
    )
    opt = df["option_type"].where(df["option_type"].isin(["CE", "PE"]), "FUT")
    strike = _num(df["strike"]).clip(lower=0) + 0.0
    compact = expiry_dt.dt.strftime("%d%b%y").str.upper()
    symbol = name + compact + "FUT"
    is_opt = opt != "FUT"
    symbol[is_opt] = (
        name[is_opt] + compact[is_opt] + strike[is_opt].map(_format_strike) + opt[is_opt]
    )
    out = _frame(
        df, symbol, df["exchange"], name, opt, expiry_dt.dt.strftime("%d-%b-%y").str.upper()
    )
    out["strike"] = strike.where(is_opt, 0.0)
    return out


def _index_symbols(names, mapping):
    upper = names.str.upper().str.strip()
    return upper.map(mapping).fillna(upper.str.replace(r"[ -]", "", regex=True))


def process_nse(df):
    idx = df[df["segment"] == "INDICES"]
    eq = df[(df["segment"] != "INDICES") & (df["instrument_name"] != "")]
    eq_symbol = eq["trading_symbol"].str.replace(r"-EQ$", "", regex=True)
    return [
        _frame(eq, eq_symbol, "NSE", eq["company_name"], "EQ"),
        _frame(
            idx,
            _index_symbols(idx["trading_symbol"], NSE_INDEX_MAP),
            "NSE_INDEX",
            idx["trading_symbol"],
            "EQ",
        ),
    ]


def process_bse(df):
    idx = df[df["segment"] == "IDX"]
    eq = df[df["instrument_name"] == "E"]
    # BSE suffixes the group: SBIN-A, BAJAJ-AUTO-A. Strip only the last part.
    eq_symbol = eq["trading_symbol"].str.replace(r"-[A-Z0-9]{1,2}$", "", regex=True)
    return [
        _frame(eq, eq_symbol, "BSE", eq["company_name"], "EQ"),
        _frame(
            idx,
            _index_symbols(idx["trading_symbol"], BSE_INDEX_MAP),
            "BSE_INDEX",
            idx["trading_symbol"],
            "EQ",
        ),
    ]


def process_fo(df):
    """NFO and BFO: company_name is the underlying (NIFTY, BAJAJ-AUTO, M&M, SENSEX)."""
    df = df[df["option_type"].isin(["XX", "CE", "PE"])]
    return [_derivatives(df, df["company_name"].str.strip())]


def process_mcx(df):
    idx = df[(df["instrument_name"] == "COM") & (df["segment"] == "INDICES")]
    fo = df[df["instrument_name"].isin(["FUTCOM", "FUTIDX", "OPTFUT", "OPTIDX"])]
    # company_name is blank on MCX; the underlying is everything before the YYMMM
    # expiry. Roots can carry digits (SILVER10026OCTFUT -> SILVER100), so a
    # letters-only prefix would merge SILVER100 into SILVER.
    name = (
        fo["trading_symbol"]
        .str.extract(r"^(.+?)\d{2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)", expand=False)
        .fillna(fo["trading_symbol"])
    )
    return [
        _derivatives(fo, name),
        _frame(idx, idx["trading_symbol"], "MCX_INDEX", idx["trading_symbol"], "EQ"),
    ]


PROCESSORS = {
    "NSECompactScrip.csv": process_nse,
    "BSECompactScrip.csv": process_bse,
    "NFOCompactScrip.csv": process_fo,
    "BFOCompactScrip.csv": process_fo,
    "MCXCompactScrip.csv": process_mcx,
}


def build_symtoken_frame(zip_bytes):
    frames = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        for filename, process in PROCESSORS.items():
            with archive.open(filename) as fh:
                df = pd.read_csv(fh, dtype=str, keep_default_na=False)
            df = df.apply(lambda col: col.str.strip())
            parts = process(df)
            logger.info(f"{filename}: {sum(len(p) for p in parts)} rows")
            frames.extend(parts)
    out = pd.concat(frames, ignore_index=True)[COLUMNS]
    return out.drop_duplicates(subset=["symbol", "exchange"], keep="first")


def replace_symtoken_table(df):
    """Delete and insert in one transaction so a failure keeps the previous contract."""
    try:
        SymToken.query.delete()
        db_session.bulk_insert_mappings(SymToken, df.to_dict(orient="records"))
        db_session.commit()
        logger.info(f"Replaced symtoken table with {len(df)} records")
    except Exception:
        db_session.rollback()
        raise


def master_contract_download():
    logger.info("Downloading Master Trust master contract")
    try:
        response = get_httpx_client().get(CONTRACT_URL, timeout=120.0)
        response.raise_for_status()
        replace_symtoken_table(build_symtoken_frame(response.content))
        return socketio.emit(
            "master_contract_download", {"status": "success", "message": "Successfully Downloaded"}
        )
    except Exception as e:
        logger.exception("Master Trust master contract download failed")
        return socketio.emit("master_contract_download", {"status": "error", "message": str(e)})
    finally:
        db_session.remove()


def search_symbols(symbol, exchange):
    return SymToken.query.filter(
        SymToken.symbol.like(f"%{symbol}%"), SymToken.exchange == exchange
    ).all()
