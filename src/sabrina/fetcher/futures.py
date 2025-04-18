from datetime import datetime
from ccxt import binance
from loguru import logger
from questdb.ingress import Sender


def top_long_short_position_ratio():
    """ """
    logger.info("")
    ex = binance()
    symbols = [
        symbol.get("symbol") for symbol in
        ex.fapiPublicGetExchangeInfo().get("symbols")
    ]
    rows = []
    for symbol in symbols:
        rows.extend(
            ex.fapiDataGetTopLongShortPositionRatio(
                params={"symbol": symbol, "period": "5m", "limit": 3}
            )
        )
    with Sender.from_conf() as sender:
        for row in rows:
            sender.row(
                "top_long_short_position_ratio",
                symbols={"symbol": symbol},
                columns={
                    "logShortRatio": row.get("logShortRatio"),
                    "shortAccount": row.get("shortAccount"),
                },
                at=datetime.fromtimestamp(int(row.get("timestamp") / 1000)),
            )
        sender.flush()
