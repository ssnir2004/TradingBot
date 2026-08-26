"""Thin wrapper around ib_async used by bot.py, trade.py, and cycle.py."""
import time

from ib_async import IB, MarketOrder, Stock, Trade

SETTLED_STATUSES_TIMEOUT = 20


class IBKRClient:
    def __init__(self, host: str, port: int, client_id: int, account: str | None = None):
        self.ib = IB()
        self.ib.connect(host, port, clientId=client_id)
        # IBKR rejects every order with error 435 ("You must specify an
        # account") once a login is authorized for more than one account -
        # a single-account login auto-fills it and needs nothing here, but
        # an ambiguous one needs the caller-supplied account (see
        # mode_config.ibkr_account) or this fails loudly right away rather
        # than letting every subsequent order silently get cancelled.
        # Stamped onto self.ib too so code that only has the raw ib_async
        # IB object (e.g. cycle.py's _place_stop) can still read it.
        if account:
            self.account = account
        else:
            accounts = self.ib.managedAccounts()
            if len(accounts) > 1:
                raise RuntimeError(
                    f"This IBKR login manages multiple accounts {accounts} but no "
                    "account was configured - set the matching *_IBKR_ACCOUNT_ID "
                    "env var (see mode_config.ibkr_account)."
                )
            self.account = accounts[0] if accounts else None
        self.ib.account = self.account

    def place_order(self, symbol: str, side: str, quantity: int) -> Trade:
        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = self.ib.qualifyContracts(contract)

        order = MarketOrder(side, quantity)
        order.outsideRth = True
        if self.account:
            order.account = self.account
        # Market orders must be DAY (a market order can't stay open past the
        # session). Left unset, IBKR fills the TIF from the account's Order
        # Presets — on live that resolves to GTC, which is invalid for a
        # market order and gets the whole order cancelled (error 10349:
        # "Order TIF was set to GTC based on order preset").
        order.tif = "DAY"
        trade = self.ib.placeOrder(qualified, order)

        # trade.isDone() (Filled/Cancelled/ApiCancelled/Inactive) is the
        # correct "stop polling" signal — unlike a plain "not pending"
        # check, it correctly keeps waiting through "ValidationError",
        # which ib_async can report as a transient, still-live state that
        # often resolves to Submitted/Filled moments later (see
        # OrderStatus.WorkingStates in ib_async's order.py).
        deadline = time.monotonic() + SETTLED_STATUSES_TIMEOUT
        while time.monotonic() < deadline:
            self.ib.sleep(0.5)
            if trade.isDone():
                break

        return trade

    def disconnect(self):
        self.ib.disconnect()


def scoped_positions(ib: IB) -> list:
    """ib.positions() filtered to this connection's own resolved account
    (IBKRClient stamps it as ib.account). Unfiltered, a login authorized
    for more than one account (see IBKRClient.__init__) returns every
    managed account's holdings mixed together - this is what would let a
    read of this mode's positions silently pick up (or miss) another
    account's shares in the same symbol."""
    return ib.positions(getattr(ib, "account", "") or "")


def belongs_to_account(ib: IB, acct_number: str | None) -> bool:
    """Whether an execution/order's own account attribution matches this
    connection's resolved account - for the calls that return everything
    across every managed account with no account= filter to pass
    (fills/reqExecutions/openTrades). True by default when this
    connection's account is unknown (single-account login - nothing to
    filter) or the checked value is unexpectedly empty, so this only ever
    narrows results, never silently drops one for a reason unrelated to
    account mismatch."""
    account = getattr(ib, "account", None)
    if not account or not acct_number:
        return True
    return acct_number == account
