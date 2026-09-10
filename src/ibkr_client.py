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


def act_on_order_any_client(ib: IB, order, action) -> None:
    """Runs `action(target_ib, order)` (a cancelOrder or a same-orderId
    placeOrder "modify") against whichever IBKR API client actually owns
    `order` (an Order object from reqAllOpenOrders()/ib.openTrades(),
    possibly placed by a DIFFERENT client than `ib` itself).

    IBKR only accepts a cancel or in-place modify of an order from the
    exact client id that originally placed it - reqAllOpenOrders()/
    openTrades() are account-wide for READING (any client sees every
    order), but acting on one some OTHER client placed silently fails:
    IBKR logs "Error 10147: OrderId X that needs to be cancelled is not
    found", the order flips to PendingCancel, then reverts right back to
    its prior working status - no exception is raised, nothing in this
    codebase was checking for it. Client id 0 does NOT have a special
    "manage anything" privilege on this account either - verified
    directly (2026-09-10) with a disposable test order; it hit the exact
    same silent failure as any other non-owning client id.

    A real live incident (2026-09-10, CHTR) hit this through cycle.py's
    own stop-repositioning: a dashboard-placed stop (modify_stop.py, its
    own client id) needed to be replaced by the engine's automatic hard-
    stop backfill (a different client id) - the cancel silently failed,
    leaving BOTH the old and the new stop resting on the same 10 shares
    at once.

    If `order.clientId` already matches `ib`'s own client id, runs
    `action` directly on the existing connection (the common case - a
    script acting on an order it placed itself in this same run needs
    nothing special). Otherwise opens a brief, separate connection using
    the order's own clientId (reusing `ib`'s host/port/account) purely to
    run `action`, then disconnects immediately - IBKR orderIds here are
    really only ever one of this codebase's own small set of well-known
    per-script client ids (see .env.example), never some arbitrary
    external value, so this is a bounded, predictable reconnect."""
    if order.clientId == ib.client.clientId:
        action(ib, order)
        ib.sleep(2)
        return

    owner = IB()
    try:
        owner.connect(ib.client.host, ib.client.port, clientId=order.clientId, timeout=10)
        account = getattr(ib, "account", None)
        if account:
            owner.account = account
        action(owner, order)
        owner.sleep(2)
    finally:
        owner.disconnect()


def cancel_order_any_client(ib: IB, order) -> None:
    """act_on_order_any_client, specialized to a plain cancel - see its
    own docstring for the full reasoning (a real live incident,
    2026-09-10, CHTR)."""
    act_on_order_any_client(ib, order, lambda target_ib, o: target_ib.cancelOrder(o))


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
