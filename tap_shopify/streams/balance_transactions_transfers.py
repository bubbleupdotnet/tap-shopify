from tap_shopify.context import Context
from tap_shopify.streams.balance_transactions_base import PayoutDrivenBTStream

class BalanceTransactionsTransfers(PayoutDrivenBTStream):
    """Stream class for Shopify transfer-type balance transactions only (payout-driven)."""

    name = "balance_transactions"

    def get_query_params_for_payout(self, payout_legacy_id, cursor=None):
        params = {
            "query": f"payments_transfer_id:{payout_legacy_id} AND payout_status:'PAID' AND transaction_type:'TRANSFER'",
            "first": self.results_per_page if self.results_per_page <= 150 else 150,
        }
        if cursor:
            params["after"] = cursor
        return params

    def get_query(self):
        return """
        query GetBalanceTransactions($first: Int!, $after: String, $query: String) {
            shopifyPaymentsAccount {
                balanceTransactions(first: $first, after: $after, query: $query, sortKey: PAYOUT_DATE) {
                    edges {
                        cursor
                        node {
                            adjustmentReason
                            adjustmentsOrders {
                                amount {
                                    amount
                                    currencyCode
                                }
                                fees {
                                    amount
                                    currencyCode
                                }
                                link
                                name
                                net {
                                    amount
                                    currencyCode
                                }
                                orderTransactionId
                            }
                            amount {
                                currencyCode
                                amount
                            }
                            associatedOrder {
                                name
                                id
                            }
                            associatedPayout {
                                status
                                id
                            }
                            fee {
                                currencyCode
                                amount
                            }
                            id
                            net {
                                currencyCode
                                amount
                            }
                            sourceOrderTransactionId
                            sourceId
                            sourceType
                            transactionDate
                            type
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        }
        """

Context.stream_objects["balance_transactions_transfers"] = BalanceTransactionsTransfers
