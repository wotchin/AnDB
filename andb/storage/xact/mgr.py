import time

STATUS_ACTIVE = 0
STATUS_COMMITTED = 1
STATUS_ABORTED = 2


class TransactionManager:
    def __init__(self):
        self.active_transactions = {}

    def begin_transaction(self, transaction_id):
        if transaction_id not in self.active_transactions:
            self.active_transactions[transaction_id] = {
                'status': STATUS_ACTIVE,
                'start_time': time.time(),
                'last_lsn': None,
                'undo_log': []
            }

    def commit_transaction(self, transaction_id):
        if transaction_id in self.active_transactions:
            transaction = self.active_transactions[transaction_id]
            if transaction['status'] == STATUS_ACTIVE:
                transaction['status'] = STATUS_COMMITTED
                transaction['last_lsn'] = self._generate_lsn()
                self._flush_transaction(transaction_id)

    def abort_transaction(self, transaction_id):
        if transaction_id in self.active_transactions:
            transaction = self.active_transactions[transaction_id]
            if transaction['status'] == STATUS_ACTIVE:
                transaction['status'] = STATUS_ABORTED
                transaction['last_lsn'] = self._generate_lsn()
                self._undo_transaction(transaction_id)

    def _generate_lsn(self):
        # TODO: Implement the generation of Log Sequence Number (LSN)
        return int(time.time())

    def _flush_transaction(self, transaction_id):
        # TODO: Implement the flushing of modified pages and logs associated with the transaction
        pass

    def _undo_transaction(self, transaction_id):
        # TODO: Implement the undoing of modifications made by the transaction using the undo log
        pass

