from tap_shopify.context import Context
from tap_shopify.streams.balance_transactions_base import PayoutDrivenBTStream

class BalanceTransactions(PayoutDrivenBTStream):
    """Stream class for all Shopify balance transactions (payout-driven)."""

    name = "balance_transactions"

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

Context.stream_objects["balance_transactions"] = BalanceTransactions
