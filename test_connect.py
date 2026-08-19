from ib_async import IB

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=99)
print('Connected:', ib.isConnected())
print('Accounts:', ib.managedAccounts())
ib.disconnect()
